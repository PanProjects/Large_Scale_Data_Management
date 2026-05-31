# Ζητούμενο 6 – Στρατηγικές Join: hint & explain για Query 3 και Query 4

---



## Α) Στρατηγικές Join που επιλέγει αυτόματα ο Catalyst

Ο Catalyst optimizer του Spark επιλέγει physical join strategy με βάση τον τύπο του join (equi vs. cross), το εκτιμώμενο μέγεθος των δεδομένων και το διαθέσιμο threshold auto-broadcast. Για τα δύο queries της εργασίας, η default επιλογή είναι διαφορετική λόγω της φύσης των joins. 

**Query 3:**
Όπως έχουμε ήδη αναφέρει, το Query 3 εκτελεί equi-join μεταξύ του census dataset (zipstats, μεγάλος πίνακας με περίπου 2.500 ZIP codes) και του income dataset (με περίπου 200 γραμμές). Επειδή ο income πίνακας είναι πολύ μικρός (< 1 KB, πολύ μικρότερος από το threshold των 10 MB), ο Catalyst επιλέγει αυτόματα BroadcastHashJoin, ο income πίνακας συλλέγεται στον driver και αποστέλλεται (broadcast) σε κάθε executor, όπου γίνεται hash lookup τοπικά χωρίς shuffle του μεγάλου πίνακα. Αυτό επαληθεύεται και από τον κώδικα του Q3df.py (ερώτημα 4), όπου χρησιμοποιείται F.broadcast(income) για να γίνει ο ίδιος επιλογέας hint ρητά. 

**Query 4:**
Επίσης, ξέρουμε ήδη πως το Query 4 εκτελεί cross join μεταξύ του crime dataset (με περίπου 2M γραμμές) και των police stations (21 γραμμές). Για cross join δεν υπάρχει equi-join key, οπότε δεν εφαρμόζονται BroadcastHashJoin, SortMergeJoin και ShuffledHashJoin. Ο Catalyst επιλέγει αυτόματα BroadcastNestedLoopJoin: οι 21 stations αποστέλλονται σε κάθε executor, και για κάθε crime record γίνεται nested loop τοπικά πάνω στα 21 stations,  χωρίς κανένα shuffle του μεγάλου crime dataset.


---

## Β) Αποτελέσματα Στρατηγικών Join

Απενεργοποιήσαμε το auto-broadcast στο Spark, ώστε τα hints για MERGE και SHUFFLE_HASH να τηρηθούν από τον Catalyst και να μην επικαλυφθούν αυτόματα. Σε κάθε query δοκιμάστηκαν και οι τέσσερις στρατηγικές που έχουν ζητηθεί με .hint(), και το φυσικό  πλάνο εκτέλεσης εκτυπώθηκε με .explain("formatted").

### Query 3 — census ⟕ income (equi-join)

| Hint | Physical Plan | Run 1 (s) | Run 2 (s) | Run 3 (s) | Μέσος Όρος (s) |
|---|---|---|---|---|---|
| `BROADCAST` | BroadcastHashJoin | 8.644 | 9.413 | 10.124 | **9.394** |
| `MERGE` | SortMergeJoin | 7.771 | 8.759 | 13.078 | 9.869 |
| `SHUFFLE_HASH` | ShuffledHashJoin | 7.824 | 13.528 | 9.513 | 10.288 |
| `SHUFFLE_REPLICATE_NL` | CartesianProduct | 10.027 | 16.372 | 10.617 | 12.339 |

### Query 4 — crimes × stations (cross join)

| Hint | Physical Plan | Run 1 (s) | Run 2 (s) | Run 3 (s) | Μέσος Όρος (s) |
|---|---|---|---|---|---|
| `BROADCAST` | BroadcastNestedLoopJoin | 13.461 | 12.998 | 13.192 | 13.217 |
| `MERGE` | CartesianProduct (fallback) | 10.933 | 11.416 | 11.915 | 11.421 |
| `SHUFFLE_HASH` | CartesianProduct (fallback) | 11.081 | 11.757 | 10.771 | **11.203** |
| `SHUFFLE_REPLICATE_NL` | CartesianProduct | 10.809 | 10.602 | 10.756 | **10.722** |

---

## Γ) Σχολιασμός αποτελεσμάτων

Στο Query 3 η αναμενόμενη ιεράρχιση από το βέλτιστο στο λιγότερο αποδοτικό είναι BROADCAST – SHUFFLE HASH – MERGE – SHUFFLE REPLICATE NL γιατί με κάθε επιπλέον shuffle που κάνουμε έχουμε επιπλέον κόστος. Το SHUFFLE REPLICATE NL είναι ξεκάθαρα η χειρότερη επιλογή καθώς ο Catalyst εφαρμόζει CartesianProduct με join condition, δηλαδή nested loop O(N × 200) αντί hash lookup που δεν είναι καθόλου αποδοτικό σε equi-joins.

Στο Query 4, τα hints MERGE και SHUFFLE_HASH δεν έχουν καμία επίδραση στο physical plan: ο Catalyst εκδίδει warning και εκτελεί CartesianProduct και στις δύο περιπτώσεις. Οι μόνες ουσιαστικά διαφορετικές στρατηγικές είναι BROADCAST (Broadcast Nested  Loop Join) και SHUFFLE_REPLICATE_NL (CartesianProduct). Επίσης στο Query 4 το BROADCAST (BroadcastNestedLoopJoin) είναι πάντα το αργότερο (avg 13.217 s), ενώ το SHUFFLE_REPLICATE_NL (CartesianProduct) είναι πάντα το ταχύτερο (avg 10.722 s). Επειδή τα crimes είναι cached στη μνήμη, το CartesianProduct διαβάζει απευθείας από τη μνήμη χωρίς το overhead του broadcast exchange, ενώ το Broadcast Nested Loop Join εισάγει ένα επιπλέον BroadcastExchange βήμα ακόμα και για 21 rows. Ωστόσο γενικά το Broadcast θεωρείται καλύτερο από το Shuffle Replicate NL γιατί δεν έχει το overhead του shuffle.
