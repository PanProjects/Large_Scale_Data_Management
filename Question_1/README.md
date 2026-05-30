# Ζητούμενο 1 – Μορφές Δεδομένων: CSV vs Parquet

## Ερώτημα

> Τα δεδομένα αποθηκεύονται κυρίως σε μορφή CSV. Είναι αυτό το πιο κατάλληλο format για επεξεργασία με Spark; Ποια εναλλακτικά formats γνωρίζετε και πώς διαφέρουν; Επιλέξτε ένα, μετατρέψτε τα δεδομένα και συγκρίνετε την επίδοση (χρόνο εκτέλεσης) σε σχέση με το CSV για κάποιο Query.

---

## Θεωρητικό Μέρος 

### Είναι το CSV κατάλληλο για Spark;

Στην εργασία τα δεδομένα είναι αποθηκευμένα σε μορφή CSV (Comma Separated Values), ένα από τα πιο ευρέως διαδεδομένα φορμάτ row-based αποθήκευσης δεδομένων. Ωστόσο δεν είναι η βέλτιστη επιλογή για επεξεργασία big data σε Spark Cluster, και η καταλληλότερη επιλογή θα ήταν να πάμε σε μια columnar επιλογή και συγκεκριμένα το Parquet. Αυτό γιατί:

1)	Τo CSV ως row-based δεν υποστηρίζει column pruning όπως το Parquet, το οποίο το υποστηρίζει natively o Spark Catalyst. Με το Column Pruning, αν το query μας χρειάζεται μόνο μερικές από τις στήλες, δεν θα χρειαστεί να τις φορτώσουμε όλες, αποφεύγοντας το διάβασμα «αχρείαστων» δεδομένων.
2)	Το Parquet φέρει ενσωματωμένο schema ώστε να μην πρέπει να κάνουμε ένα full read των δεδομένων για να δούμε τον τύπο τους, όπως επίσης και ότι φέρει metadata aggregate στατιστικών των data και υποστηρίζουν predicate pushdown βελτιστοποιήσεις δηλαδή να μπορούμε να κάνουμε φιλτράρισμα πριν τη πλήρη φόρτωση στη μνήμη, μειώνοντας τον κόστος σε I/Os.
3)	Τέλος τα data στο parquet είναι πιο καλά συμπιεσμένα και δομημένα σε αντίθεση με το CSV όπου στερούνται ταχύτητας σε parsing και αποδοτικού encoding.

Εναλλακτικά formats πέραν του parquet, είναι το ORC (Optimized Row Columnar). Είναι κι αυτό columnar και ενδείκνυται για χρήση σε cluster με χαρακτηριστικά όπως ενσωματωμένα indexes, lightweight indexing (βλ. stripes) και πολύ καλή συμπίεση, ωστόσο η κύρια διαφορά του με το parquet είναι ότι είναι optimized για Apache Hive, ενώ το Parquet για Spark. Τέλος αναζητώντας εναλλακτικά format στο διαδίκτυο, αξίζει να αναφερθούμε στο Avro, το οποίο βέβαια είναι row-based, δεν είναι ιδανικό για OLAP που αντιμετωπίζουμε στην προκειμένη περίπτωση, αλλά είναι πολύ καλό για streaming & message serialization σε εργασίες που κάνουμε read/write ολόκληρα records. Το Avro αποθηκεύει το schema μαζί με τα data (υποστηρίζει schema evolution), που το κάνει καλή επιλογή σε Kafka pipelines.


---

## Εκτέλεση Κώδικα

### Scripts

| Αρχείο | Τι κάνει το αρχείο |
|---|---|
| `Q1_csv_to_parquet.py` | Μετατροπή CSV σε Parquet |
| `Q1_csv.py` | Query 1 διαβάζοντας από CSV |
| `Q1_parquet.py` | Query 1 διαβάζοντας από Parquet |

Και τα τρία εκτυπώνουν `QUERY_ELAPSED_SECONDS=<sec>`, ο χρόνος που καταγράφεται στη σύγκριση.

---

### Βήμα 1 – Φόρτωση περιβάλλοντος

```bash
source ~/bigdata-env.sh
echo $DSML_USER   # πρέπει να εμφανίσει π.χ. dsml00305
```

---

### Βήμα 2 – Upload scripts στο HDFS

```bash
hadoop fs -mkdir -p hdfs://hdfs-namenode.default.svc.cluster.local:9000/user/${DSML_USER}/code

hadoop fs -put -f Q1_csv_to_parquet.py Q1_csv.py Q1_parquet.py \
    hdfs://hdfs-namenode.default.svc.cluster.local:9000/user/${DSML_USER}/code/
```

---

### Βήμα 3 – Μετατροπή CSV σε Parquet (μία φορά)

```bash
spark-submit \
    --conf spark.app.name=Q1_CSV_to_Parquet \
    --conf spark.executor.instances=4 \
    --conf spark.executor.cores=1 \
    --conf spark.executor.memory=2g \
    hdfs://hdfs-namenode.default.svc.cluster.local:9000/user/${DSML_USER}/code/Q1_csv_to_parquet.py
```

Επαληθεύουμε ότι έγινε η μετατροπή σε Parquet:

```bash
hadoop fs -ls hdfs://hdfs-namenode.default.svc.cluster.local:9000/user/${DSML_USER}/data/LA_Crime_Parquet/
```

---

### Βήμα 4 – Εκτέλεση Query 1 σε CSV

```bash
spark-submit \
    --conf spark.app.name=Q1_CSV \
    --conf spark.executor.instances=4 \
    --conf spark.executor.cores=1 \
    --conf spark.executor.memory=2g \
    hdfs://hdfs-namenode.default.svc.cluster.local:9000/user/${DSML_USER}/code/Q1_csv.py
```

Από τα logs καταγράφουμε τον χρόνο εκτέλεσης QUERY_ELAPSED_SECONDS = 6.625 seconds

---

### Βήμα 5 – Εκτέλεση Query 1 σε Parquet

```bash
spark-submit \
    --conf spark.app.name=Q1_Parquet \
    --conf spark.executor.instances=4 \
    --conf spark.executor.cores=1 \
    --conf spark.executor.memory=2g \
    hdfs://hdfs-namenode.default.svc.cluster.local:9000/user/${DSML_USER}/code/Q1_parquet.py
```

Από τα logs καταγράφουμε: QUERY_ELAPSED_SECONDS=5.070

---

### Βήμα 6 – Ανάκτηση logs από k9s ή kubectl

```bash
# pods:
kubectl get pods -n ${DSML_USER}-priv --sort-by=.metadata.creationTimestamp

# logs:
kubectl logs <driver-pod-name> -n ${DSML_USER}-priv | grep "QUERY_ELAPSED\|segment\|Night\|Morning"
```

Μπορούμε να αποθηκεύσουμε τα logs με k9s (`l` → `s`) στο:
```
~/.local/state/k9s/screen-dumps/cluster.local/${DSML_USER}-priv/
```

---

## Μέρος Γ – Αποτελέσματα

### Query 1 – Αποτέλεσμα (ίδιο και στα δύο formats)

```
+---------+-----------+----------+
|  segment|crime_count|percentage|
+---------+-----------+----------+
|    Night|     251094|     34.08|
|  Evening|     198292|     26.92|
|Afternoon|     156432|     21.23|
|  Morning|     130866|     17.76|
+---------+-----------+----------+
```

### Πίνακας σύγκρισης επίδοσης

| Format | Χρόνος Query 1 (sec) | Μέγεθος δεδομένων |
|---|---|---|
| CSV | 6.625 | ~1.8 GB |
| Parquet | 5.070| ~300–400 MB (Snappy) |

---

Η διαφορά είναι μεν μετρήσιμη, αλλά όχι πολύ μεγάλη γιατί το dataset είναι σχετικά μικρό (λιγότερο από 1GB) και το query του ερωτήματος είναι σχετικά απλό (filter + group + sort) δεν κάναμε κάποιο βαρύ projection (πχ SELECT 2 από 28 στήλες) για να φανεί η αξία του column pruning. Παρόλα αυτά έχουμε 23% speedup με το parquet που δείχνει ότι πράγματι είναι πιο αποδοτικό format.
