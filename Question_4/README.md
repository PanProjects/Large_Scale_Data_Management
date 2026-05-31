# Ζητούμενο 4 – Query 3: DataFrame και RDD APIs

## 4.1 Περιγραφή Query 3

Για κάθε ZIP Code του Los Angeles, υπολογίζεται το **εκτιμώμενο μέσο ετήσιο κατακεφαλήν εισόδημα** για τη διετία 2020–2021, συνδυάζοντας:

- **Δεδομένα απογραφής 2020** (`LA_Census_Blocks_2020.geojson`): πληθυσμός και αριθμός κατοικιών ανά census block
- **Δεδομένα εισοδήματος 2021** (`LA_income_2021.csv`): μέσο εισόδημα νοικοκυριού ανά ZIP code

### Τύπος υπολογισμού

```
per_capita_income = median_household_income × (total_housing_units / total_population)
```

**Σκεπτικό:**

```
median_household_income        (εισόδημα ανά νοικοκυριό)
÷  (total_population / total_housing_units)   (μέσο μέγεθος νοικοκυριού)
=  median_household_income × (housing_units / population)
≈  εισόδημα ανά άτομο
```

### Αλγόριθμος

```
1. Φόρτωση GeoJSON census blocks (multiLine JSON)
2. Flatten properties → top-level columns
3. GROUP BY zip: SUM(population), SUM(housing_units)
4. Φόρτωση income CSV (separator = ";"), καθαρισμός τιμών ($, ,)
5. JOIN census ⟕ income ON zip
6. per_capita = median_income × (housing_units / population)
7. ORDER BY zip ASC
```

---

## 4.2 Υλοποιήσεις

| Αρχείο | API | Join τεχνική |
|---|---|---|
| `Q3_df.py` | DataFrame | `broadcast(income_df)`: μικρό dataset, αποφεύγει shuffle |
| `Q3_rdd.py` | RDD | `sc.broadcast(income_dict)`: collect + dict lookup, αποφεύγει `join()` shuffle |

---

## 4.3 Ανάλυση υλοποιήσεων

### Υλοποίηση Α: DataFrame API (`Q3_df.py`)

Το execution plan έχει τρία κύρια στάδια:

**1. GeoJSON φόρτωση και aggregation:**
```python
zip_stats = (
    blocks
    .select(F.col(COL_ZIP), F.col(COL_POP).cast(LongType()), F.col(COL_HU).cast(LongType()))
    .filter(...)
    .groupBy("zip")
    .agg(F.sum("population"), F.sum("housing_units"))
)
```
Αυτό παράγει ένα shuffle για το `groupBy`, κάθε executor στέλνει τα δεδομένα του ZIP στον "σωστό" partition.

**2. Broadcast Join:**
```python
result_df = zip_stats.join(F.broadcast(income), "zip")
```
Το income dataset έχει ~200 ZIP codes (~10 KB). Ο Catalyst το στέλνει ολόκληρο σε κάθε executor (**broadcast**), αποφεύγοντας ένα δεύτερο shuffle. Αυτό είναι αυτόματο αν το dataset είναι < `spark.sql.autoBroadcastJoinThreshold` (default 10 MB), αλλά το κάνουμε ρητό με `F.broadcast()` για ασφάλεια.

**3. Υπολογισμός και εγγραφή:**
```python
.withColumn("per_capita_income",
    F.round(F.col("median_income") * (F.col("total_hu") / F.col("total_pop")), 2))
```
Τοπικός υπολογισμός ανά partition, χωρίς shuffle.

### Υλοποίηση Β: RDD API (`Q3_rdd.py`)

Το ίδιο πρόβλημα λύνεται με RDD transformations:

**1. Census RDD:**
```python
census_rdd = flat_df.select(COL_ZIP, COL_POP, COL_HU).rdd
    .filter(...)
    .map(lambda r: (zip, (pop, hu)))
```

**2. Aggregation με `reduceByKey`:**
```python
zip_stats_rdd = census_rdd.reduceByKey(lambda a, b: (a[0]+b[0], a[1]+b[1]))
```
Εκτελεί **partial aggregation** ανά partition (combiner) πριν το shuffle, μειώνοντας τα δεδομένα που μεταφέρονται.

**3. Broadcast Join με dict:**
Αντί για `rdd.join(income_rdd)` (που προκαλεί shuffle και στα δύο RDDs), το income dataset συλλέγεται στον driver και μεταδίδεται ως `broadcast variable`:
```python
income_dict = dict(income_rdd.collect())          # collect 200 rows στον driver
income_broadcast = sc.broadcast(income_dict)       # αποστολή σε κάθε executor

result_rdd = zip_stats_rdd.map(
    lambda kv: (..., income_broadcast.value.get(kv[0]))  # dict lookup, χωρίς shuffle
)
```

Αυτή η τεχνική **αντικαθιστά το RDD join** για μικρά datasets και είναι σημαντικά ταχύτερη από `stats_rdd.join(income_rdd)`.

---

## 4.5 Εκτέλεση

**Configuration:** 3 executors × 1 core × 2 GB memory

```bash
spark-submit \
    --conf spark.executor.instances=3 \
    --conf spark.executor.cores=1 \
    --conf spark.executor.memory=2g \
    hdfs://hdfs-namenode.default.svc.cluster.local:9000/user/${DSML_USER}/code/Q3_df.py
```

---

## Step 3 – Run Query 3: RDD API

```bash
spark-submit \
    --conf spark.executor.instances=3 \
    --conf spark.executor.cores=1 \
    --conf spark.executor.memory=2g \
    hdfs://hdfs-namenode.default.svc.cluster.local:9000/user/${DSML_USER}/code/Q3_rdd.py
```

---

## 4.6 Αποτελέσματα

### Χρόνοι εκτέλεσης

| Υλοποίηση | Run 1 (sec) | Run 2 (sec) | Run 3 (sec) | Average (sec) |
|---|---|---|---|---|
| DataFrame | 19.150 | 20.710 | 20.086 | **19.982** |
| RDD | 26.052 | 25.845 | 21.629 | **24.509** |

### Δείγμα αποτελέσματος (20 πρώτες εγγραφές)

| zip   | total_pop | total_hu | median_income | per_capita_income |
|--------|----------:|----------:|--------------:|------------------:|
| 90001 | 55,859    | 13,820    | 52,806.0      | 13,064.66         |
| 90002 | 53,150    | 13,036    | 46,159.0      | 11,321.33         |
| 90003 | 72,764    | 18,244    | 47,733.0      | 11,968.02         |
| 90004 | 58,585    | 24,944    | 54,947.0      | 23,395.03         |
| 90005 | 37,987    | 18,721    | 44,913.0      | 22,134.32         |
| 90006 | 58,229    | 21,425    | 41,068.0      | 15,110.72         |
| 90007 | 40,944    | 14,971    | 33,222.0      | 12,147.48         |
| 90008 | 33,041    | 15,112    | 49,379.0      | 22,584.53         |
| 90010 | 5,400     | 3,152     | 76,547.0      | 44,680.77         |
| 90011 | 102,308   | 24,348    | 47,126.0      | 11,215.39         |
| 90012 | 33,851    | 15,327    | 53,278.0      | 24,123.13         |
| 90013 | 15,589    | 9,420     | 22,291.0      | 13,469.83         |
| 90014 | 9,254     | 6,788     | 31,332.0      | 22,982.67         |
| 90015 | 27,324    | 14,593    | 53,062.0      | 28,338.96         |
| 90016 | 46,512    | 17,716    | 53,659.0      | 20,438.23         |
| 90017 | 27,295    | 15,191    | 44,607.0      | 24,825.97         |
| 90018 | 50,179    | 17,553    | 55,275.0      | 19,335.62         |
| 90019 | 62,002    | 25,266    | 61,616.0      | 25,108.70         |
| 90020 | 38,694    | 18,516    | 51,013.0      | 24,410.93         |
| 90021 | 5,192     | 1,725     | 25,364.0      | 8,426.98          |

---

## 4.7 Σχολιασμός χρόνων εκτέλεσης

### Παρατηρούμενη κατάταξη

```
- DataFrame (19.982 sec)
- RDD (24.509 sec)
```

Το DataFrame API είναι ελαφρώς από το RDD API (διαφορά ~4.5 sec στον μέσο χρόνο) σχεδόν ίδια περίπου.

Αυτό πιθανώς οφείλεται στο ότι ο Catalyst optimizer εκτελεί όλη την aggregation εντός της JVM με generated bytecode (whole-stage codegen) και αξιοποιεί projection pushdown κατά την ανάγνωση, ενώ η RDD υλοποίηση πληρώνει το κόστος διέλευσης κάθε εγγραφής από τη γέφυρα Python και JVM για την εκτέλεση των lambdas.

Παρόλα αυτά, η διαφορά μένει σχετικά μικρή για δύο λόγους: πρώτον, το κυρίαρχο κόστος του query είναι η φόρτωση και αποσυμπίεση του GeoJSON, ίδια και στις δύο υλοποιήσεις (και οι δύο περνούν από τον DataFrame JSON reader), οπότε ένα μεγάλο τμήμα του χρόνου είναι εξ ορισμού μη βελτιώσιμο και επίσης, το τελικό αποτέλεσμα είναι μόλις περίπου 200 ZIP codes, άρα το Python IPC overhead του reduceByKey εφαρμόζεται σε πολύ λίγες εγγραφές, σε αντίθεση με το Ζητούμενο 2 (όπου είχαμε 2 εκατ. crime records), όπου το ίδιο overhead πολλαπλασιάζεται και το χάσμα διευρύνεται δραματικά.
