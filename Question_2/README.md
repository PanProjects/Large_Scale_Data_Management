# Ζητούμενο 2 – Query 1: DataFrame (no UDF), DataFrame (UDF) και RDD APIs

## 2.1 Περιγραφή Query 1

Το Query 1 ταξινομεί τα τμήματα της ημέρας σε φθίνουσα σειρά, ανάλογα με το **ποσοστό εγκλημάτων που έλαβαν χώρα σε δρόμο (STREET)** ως προς το σύνολο των street crimes.

### Ορισμός τμημάτων ημέρας

| Τμήμα | Ώρες (HHMM format) |
|---|---|
| Πρωί (Morning) | 0500 – 1159 |
| Απόγευμα (Afternoon) | 1200 – 1659 |
| Βράδυ (Evening) | 1700 – 2059 |
| Νύχτα (Night) | 2100 – 2359 **και** 0000 – 0459 |

Η στήλη `TIME OCC` αποθηκεύεται ως ακέραιος σε HHMM format (π.χ. 1430 = 14:30).

### Η λογική του αλγορίθμου μας

```
1. Φόρτωση και των δύο CSV (2010-2019, 2020-2025)
2. Φίλτρο: Premis Desc == "STREET"
3. Ανάθεση segment ανά γραμμή βάσει TIME OCC
4. COUNT(*) GROUP BY segment
5. percentage = count_segment / total_street_crimes × 100
6. ORDER BY percentage DESC
```

---

## 2.2 Υλοποιήσεις

Αναπτύχθηκαν τρεις ισοδύναμες υλοποιήσεις που παράγουν **ακριβώς το ίδιο αποτέλεσμα**:

| Αρχείο | API | Μέθοδος ανάθεσης segment |
|---|---|---|
| `Q1_df.py` | DataFrame (χωρίς UDF) | `F.when()` - native Spark Column expressions |
| `Q1_df_udf.py` | DataFrame (με UDF) | Python function - `F.udf()` |
| `Q1_rdd.py` | RDD | Python lambda + `reduceByKey()` |

---

## 2.3 Ανάλυση κάθε υλοποίησης

### Υλοποίηση Α: DataFrame χωρίς UDF (`Q1_df.py`)

Χρησιμοποιεί αποκλειστικά **native Spark Column expressions** μέσω του `F.when().otherwise()` API:

```python
segmented = street_df.withColumn(
    "segment",
    F.when((F.col("TIME OCC") >= 500)  & (F.col("TIME OCC") <= 1159), "Morning")
     .when((F.col("TIME OCC") >= 1200) & (F.col("TIME OCC") <= 1659), "Afternoon")
     .when((F.col("TIME OCC") >= 1700) & (F.col("TIME OCC") <= 2059), "Evening")
     .when((F.col("TIME OCC") >= 2100) | (F.col("TIME OCC") <= 459),  "Night")
     .otherwise(None)
)
```
Τα F.when() expressions μεταφράζονται σε Catalyst logical plan nodes και παραμένουν εξ ολοκλήρου στο JVM. Ο Catalyst optimizer μπορεί να τα ενοποιήσει με άλλα φίλτρα, να εφαρμόσει whole-stage code generation και να παράγει bytecode που εκτελείται σε tight loop χωρίς Python overhead. Τέλος, δεν υπάρχει καμία μεταφορά δεδομένων μεταξύ JVM και Python workers.

### Υλοποίηση Β: DataFrame με UDF (`Q1_df_udf.py`)

Ορίζει Python function και την καταχωρεί ως UDF:

```python
def assign_segment(time_occ):
    if time_occ is None:
        return None
    if 500 <= time_occ <= 1159:
        return "Morning"
    if 1200 <= time_occ <= 1659:
        return "Afternoon"
    if 1700 <= time_occ <= 2059:
        return "Evening"
    if time_occ >= 2100 or time_occ <= 459:
        return "Night"
    return None

segment_udf = F.udf(assign_segment, StringType())
segmented = street_df.withColumn("segment", segment_udf(F.col("TIME OCC")))
```
Η λογική του κώδικα είναι ως εξής: Ο Catalyst βλέπει το UDF ως ένα black box  δεν μπορεί να ελέγξει τη λογική εντός του. Για κάθε γραμμή: η τιμή TIME OCC σειριοποιείται από το JVM, αποστέλλεται στο Python worker process μέσω IPC (inter-process communication), εκτελείται η Python function, και το αποτέλεσμα επιστρέφεται στο JVM. Αυτό επαναλαμβάνεται για κάθε μία από τις γραμμές (συσσωρεύεται overhead). Επίσης, δεν εφαρμόζεται whole-stage code gen για το τμήμα που αφορά το UDF.

### Υλοποίηση Γ: RDD API (`Q1_rdd.py`)

Χρησιμοποιεί την RDD chain transformation/action μοντέλο:

```python
# Χρήση του DataFrame reader για σωστό CSV parsing, μετά μετάβαση σε RDD
rdd = spark.read.csv(...).rdd

street_rdd = rdd.filter(
    lambda row: row["Premis Desc"] is not None
                and row["Premis Desc"].upper() == "STREET"
)

segment_rdd = (
    street_rdd
    .map(lambda row: (assign_segment(row["TIME OCC"]), 1))
    .filter(lambda kv: kv[0] is not None)
)

counts_rdd = segment_rdd.reduceByKey(lambda a, b: a + b)
```

Εδώ τώρα, δεν υπάρχει Catalyst optimizer, το execution plan είναι ακριβώς αυτό που γράφουμε. Οι Python lambdas εκτελούνται στο Python worker process (παρόμοιο IPC overhead με UDF). Η reduceByKey() εκτελεί partial aggregation ανά partition πριν τo shuffle, αποδοτικά. 

---

## 2.4 Εκτέλεση

**Configuration:** 2 executors × 1 core × 2 GB memory

```bash
# DataFrame (no UDF)
spark-submit \
  --conf spark.executor.instances=2 \
  --conf spark.executor.cores=1 \
  --conf spark.executor.memory=2g \
  --conf spark.app.name=Q1_DF_noUDF \
  hdfs://hdfs-namenode.default.svc.cluster.local:9000/user/$DSML_USER/code/Q1_df.py \
  --base-path hdfs://hdfs-namenode.default.svc.cluster.local:9000/user/$DSML_USER

# DataFrame (with UDF)
spark-submit \
  --conf spark.executor.instances=2 \
  --conf spark.executor.cores=1 \
  --conf spark.executor.memory=2g \
  --conf spark.app.name=Q1_DF_UDF \
 hdfs://hdfs-namenode.default.svc.cluster.local:9000/user/$DSML_USER/code/Q1_df_udf.py \
  --base-path hdfs://hdfs-namenode.default.svc.cluster.local:9000/user/$DSML_USER

# RDD
spark-submit \
  --conf spark.executor.instances=2 \
  --conf spark.executor.cores=1 \
  --conf spark.executor.memory=2g \
  --conf spark.app.name=Q1_RDD \
  hdfs://hdfs-namenode.default.svc.cluster.local:9000/user/$DSML_USER/code/Q1_rdd.py \
  --base-path hdfs://hdfs-namenode.default.svc.cluster.local:9000/user/$DSML_USER
```

---

## 2.5 Αποτελέσματα

### Αποτέλεσμα Query 1

| Τμήμα ημέρας | Αριθμός εγκλημάτων | Ποσοστό (%) |
|---|---|---|
|    Night|     251094|     34.08|
|  Evening|     198292|     26.92|
|Afternoon|     156432|     21.23|
|  Morning|     130866|     17.76|

Παρατηρούμε πως κατά την διάρκεια της νύχτας προκύπτει το 34% των εγκλημάτων, και το απόγευμα με 27%. Είναι φανερό πως κατά το απόγευμα-βράδι έχουμε μεγαλύτερη συχνότητα εμφάνισης εγκλημάτων.

### Χρόνοι εκτέλεσης

| Υλοποίηση          | Run 1 (sec) | Run 2 (sec) | Run 3 (sec) | Average (sec) |
| ------------------ | ----------: | ----------: | ----------: | ------------: |
| DataFrame (no UDF) |      18.213 |      29.041 |      34.083 |        27.112 |
| DataFrame (UDF)    |      39.522 |      47.274 |      38.795 |        41.864 |
| RDD                |      85.638 |      53.035 |      72.002 |        70.225 |


Η υλοποίηση Dataframe χωρίς την UDF είναι 1.54 φορές ταχύτερη από αυτή με UDF και 2.59 φορές ταχύτερη από την RDD υλοποίηση.  

---

## Σχολιασμός αποτελεσμάτων
Τα αποτελέσματα επιβεβαιώνουν την θεωρία μας. Τα Spark native column expressions διαβάζονται από τον Catalyst ο οποίος μπορεί να βελτιστοποιήσει το λογικό σχέδιο: Μπορεί να κάνει Predicate Pushdown (Το φίλτρο Premis Desc = 'STREET' εφαρμόζεται πριν φτάσουν τα δεδομένα στο transformation stage) καθώς και να κάνει περεταίρω βελτιστοποιήσεις όπως whole-stage code generation, batch processing και μηδενικό IPC overhead. Όταν όμως εισάγουμε UDF, η UDF είναι αδιαφανής για τον optimizer (black box), δεν μπορεί να υλοποιήσει βελτιστοποιήσεις μέσα στην UDF με αποτέλεσμα την μείωση του Performance. Τέλος η RDD υλοποίηση είχε την μεγαλύτερη καθυστέρηση καθώς ούτε και αυτή δεν λειτουργεί με Catalyst, δεν έχει predicate pushdown και το overhead της RDD αφαίρεσης είναι σημαντικά υψηλότερο από του DataFrame. 

Αυτό που προτείνεται κατά την υλοποίηση του Spark προγράμματος μας είναι να χρησιμοποιούμε όσο το δυνατό γίνεται native συναρτήσεις του Spark αντί για UDFs σε OLTP queries. 
