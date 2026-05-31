# Ζητούμενο 3 - Query 2: DataFrame και Spark SQL APIs

## 3.1 Περιγραφή Query 2

Για κάθε **έτος** στο σύνολο δεδομένων, εντοπίζονται οι **3 μήνες με τον υψηλότερο αριθμό καταγεγραμμένων εγκλημάτων**. Το αποτέλεσμα εμφανίζεται ανά έτος με τους μήνες, το σύνολο περιστατικών και τη θέση κατάταξης.

Το αποτέλεσμα θα πρέπει να είναι στην παρακάτω μορφή (ενδεικτικά):

| year | month | crime_total | ranking |
|---|---|---|---|
| 2010 | 2 | 2145 | 1 |
| 2010 | 3 | 1492 | 2 |
| 2010 | 5 | 54 | 3 |
| 2011 | 12 | 4632 | 1 |
| ... | ... | ... | ... |

Ταξινόμηση: **year ASC**, **crime_total DESC** (η ranking απορρέει φυσικά).

### Η λογική του αλγορίθμου μας

```
1. Φόρτωση και των δύο CSV (2010-2019, 2020-2025)
2. Parsing ημερομηνίας: TO_TIMESTAMP("DATE OCC", 'yyyy MMM dd hh:mm:ss a')
3. Εξαγωγή YEAR και MONTH
4. GROUP BY (year, month) → COUNT(*) = crime_total
5. RANK() OVER (PARTITION BY year ORDER BY crime_total DESC)
6. FILTER ranking <= 3
7. ORDER BY year ASC, crime_total DESC
```

Η στήλη `DATE OCC` αποθηκεύεται ως string στη μορφή `"yyyy MMM dd hh:mm:ss a"` (π.χ. `"2010 Feb 20 12:00:00 AM"`). Χρησιμοποιείται `to_timestamp()` με το ακριβές format pattern:

```python
F.to_timestamp(F.col("DATE OCC"), "yyyy MMM dd hh:mm:ss a")
```

---

## 3.2 Υλοποιήσεις

| Αρχείο | API | Τεχνική κατάταξης |
|---|---|---|
| `Q2_df.py` | DataFrame | `Window.partitionBy("year").orderBy(F.col("crime_total").desc())` + `F.rank()` |
| `Q2_sql.py` | Spark SQL | `RANK() OVER (PARTITION BY year ORDER BY crime_total DESC)` σε CTE |

---

## 3.3 Ανάλυση κάθε υλοποίησης

### Υλοποίηση Α: DataFrame API (`Q2_df.py`)

Η υλοποίηση χρησιμοποιεί Window functions του DataFrame API:

```python
monthly_counts = (
    df
    .withColumn("year",  F.year(F.to_timestamp("DATE OCC", "yyyy MMM dd hh:mm:ss a")))
    .withColumn("month", F.month(F.to_timestamp("DATE OCC", "yyyy MMM dd hh:mm:ss a")))
    .filter(F.col("year").isNotNull())
    .groupBy("year", "month")
    .agg(F.count("*").alias("crime_total"))
)

year_window = Window.partitionBy("year").orderBy(F.col("crime_total").desc())

result_df = (
    monthly_counts
    .withColumn("ranking", F.rank().over(year_window))
    .filter(F.col("ranking") <= 3)
    .orderBy("year", F.col("crime_total").desc())
    .select("year", "month", "crime_total", "ranking")
)
```

Αρχικά κάνουμε parse την ημερομηνία η οποία διανέμεται σε όλους τους executors. Στη συνέχεια με groupBy("year", "month").count() κάνουμε shuffle για group aggregation. Εφαρμόζουμε την Window function RANK(), γίνεται το δεύτερο shuffle, με partition by year. Κατόπιν κάνουμε fliter με ranking <= 3, μετά την κατάταξη. Και τέλος το final sort, το τρίτο shuffle για global ordering by year.

### Υλοποίηση Β: Spark SQL (`Q2_sql.py`)

Η υλοποίηση χρησιμοποιεί CTE (Common Table Expression):

```sql
WITH monthly AS (
    SELECT
        YEAR(TO_TIMESTAMP(`DATE OCC`, 'yyyy MMM dd hh:mm:ss a'))  AS year,
        MONTH(TO_TIMESTAMP(`DATE OCC`, 'yyyy MMM dd hh:mm:ss a')) AS month,
        COUNT(*) AS crime_total
    FROM crimes
    WHERE TO_TIMESTAMP(`DATE OCC`, 'yyyy MMM dd hh:mm:ss a') IS NOT NULL
    GROUP BY
        YEAR(TO_TIMESTAMP(`DATE OCC`, 'yyyy MMM dd hh:mm:ss a')),
        MONTH(TO_TIMESTAMP(`DATE OCC`, 'yyyy MMM dd hh:mm:ss a'))
),
ranked AS (
    SELECT
        year, month, crime_total,
        RANK() OVER (PARTITION BY year ORDER BY crime_total DESC) AS ranking
    FROM monthly
)
SELECT year, month, crime_total, ranking
FROM ranked
WHERE ranking <= 3
ORDER BY year ASC, crime_total DESC
```

Για να μεταφραστεί η SQL εσωτερικά ο Spark SQL parser μετατρέπει SQL string σε AST (Abstract Syntax Tree), το οποίο στη συνέχεια μετατρέπεται σε ακριβώς το ίδιο Catalyst logical plan με αυτό που παράγει το DataFrame API. Ο ίδιος optimizer εφαρμόζει τις ίδιες βελτιστοποιήσεις και παράγει το ίδιο physical plan. Επομένως δεν αναμένουμε κάποια διαφορά στην απόδοση.

Επίσης χρησιμοποιώντας την RANK(), αν δύο μήνες έχουν ίδιο crime_total, λαμβάνουν τον ίδιο rank και ενδεχομένως εμφανίζονται περισσότερες από 3 γραμμές ανά έτος.

---

## 3.5 Εκτέλεση

**Configuration:** 4 executors × 1 core × 2 GB memory

Οι πλήρεις εντολές βρίσκονται στο αρχείο [`commands.md`](commands.md).

```bash
# DataFrame API
spark-submit \
  --conf spark.executor.instances=4 \
  --conf spark.executor.cores=1 \
  --conf spark.executor.memory=2g \
  hdfs://hdfs-namenode.default.svc.cluster.local:9000/user/${DSML_USER}/code/Q2_df.py

# Spark SQL API
spark-submit \
  --conf spark.executor.instances=4 \
  --conf spark.executor.cores=1 \
  --conf spark.executor.memory=2g \
  hdfs://hdfs-namenode.default.svc.cluster.local:9000/user/${DSML_USER}/code/Q2_sql.py
```

---

## 3.6 Αποτελέσματα

### Δείγμα αποτελεσμάτων

| year | month | crime_total | ranking |
|---|---|---|---|
|2010|    1|      19524|      1|
|2010|    3|      18131|      2|
|2010|    7|      17857|      3|
|2011|    1|      18144|      1|
|2011|    7|      17284|      2|
|2011|   10|      17035|      3|
|2012|    1|      17958|      1|
|2012|    8|      17662|      2|

### Χρόνοι εκτέλεσης

Οι δυο υλοποιήσεις δίνουν κατά μέσο όρο τους ίδιους χρόνους εκτέλεσης:

| Υλοποίηση | Run 1 (sec) | Run 2 (sec) | Run 3 (sec) | Average (sec) |
|------------|------------:|------------:|------------:|--------------:|
| DataFrame | 42.642 | 40.764 | 63.478 | 48.961 |
| SparkSQL | 49.910 | 52.431 | 55.866 | 52.736 |

---

## 3.7 Σχολιασμός χρόνων εκτέλεσης

#Έτσι το Dataframe API & Spark SQL είναι ισοδύναμα ως προς την επίδοσή τους και αυτό γιατί πρακτικά ισοδύναμες εκφράσεις οδηγούν στο ίδιο αρχικό λογικό σχέδιο στο οποίο έρχεται ο Catalyst Optimizer να αναλύσει και να βελτιστοποιήσει με αναδιάταξη (column pruning, predicate pushdowns). Έτσι λοιπόν, ανεξάρτητο το που γράφω την ίδια έκφραση, ο Catalyst κάνει την ίδια βελτιστοποίηση, και τα 2 είναι ισοδύναμα.