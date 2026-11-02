# Databricks notebook source
# MAGIC %md
# MAGIC # link_crashes: step 5 of the triage pipeline, on the tickets
# MAGIC Links the tickets that describe the same crash, so counts and spikes can count crashes, not filings:
# MAGIC 1. the same **Same Incident ID** (NHTSA's own link): same crash, linked by the rule
# MAGIC 2. a similar story (`find_pairs`, cut-off 0.9 unless changed) **and the same month, city and minute**: same crash (two vehicles in one crash)
# MAGIC 3. a similar story **1 to 10 minutes apart**, or with no time: `to check` by a triager
# MAGIC 4. a similar story **more than 10 minutes apart**: `different` (the same wording reused for another crash)
# MAGIC
# MAGIC Writes `workspace.triage.links` (one row per pair) and `workspace.triage.crashes` (each ticket and its crash).
# MAGIC A triager's decision (`decided_by = 'person'`) is never overwritten.

# COMMAND ----------

# MAGIC %pip install scikit-learn

# COMMAND ----------

# 1. Settings: the story cut-off is a box at the top of the notebook (a job can pass it too)
dbutils.widgets.text("cutoff", "0.9")
CUTOFF = float(dbutils.widgets.get("cutoff"))

# COMMAND ----------

# 2. The tickets from step 4 (no exact copies) with their time, incident id and VIN from step 3; then the story pairs
import os
import sys

sys.path.append(os.path.abspath("../scripts"))  # the repo's scripts folder, next to this notebooks folder
from find_duplicates import find_pairs  # noqa: E402

tickets = spark.sql("""
    SELECT t.key, t.month, t.city, t.story, r.incident_time, r.incident_id, r.vin
    FROM workspace.triage.tickets AS t JOIN workspace.triage.reports AS r USING (report_id)
    WHERE t.copy_of IS NULL""")
tickets.createOrReplaceTempView("t")  # the SQL cells below read it by this name

pairs = find_pairs(tickets.select("key", "month", "city", "story").toPandas(),
                   text="story", key="key", same=["month", "city"], threshold=CUTOFF)
spark.createDataFrame(pairs, schema="a STRING, b STRING, similarity DOUBLE").createOrReplaceTempView("story_pairs")
print(f"{tickets.count():,} tickets; {len(pairs)} story pairs at cut-off {CUTOFF}")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 3. Every pair and the rule that decides it
# MAGIC CREATE OR REPLACE TEMP VIEW found AS
# MAGIC WITH id_pairs AS (  -- same Same Incident ID
# MAGIC   SELECT a.key AS key_a, b.key AS key_b
# MAGIC   FROM t AS a JOIN t AS b ON a.incident_id = b.incident_id AND a.key < b.key
# MAGIC   WHERE trim(a.incident_id) <> ''
# MAGIC ),
# MAGIC story AS (  -- similar story, with how many minutes apart the two crashes are
# MAGIC   SELECT least(p.a, p.b) AS key_a, greatest(p.a, p.b) AS key_b, p.similarity,
# MAGIC     abs((hour(ta) * 60 + minute(ta)) - (hour(tb) * 60 + minute(tb))) AS gap
# MAGIC   FROM (SELECT p.*, try_to_timestamp(a.incident_time, 'H:mm') AS ta, try_to_timestamp(b.incident_time, 'H:mm') AS tb
# MAGIC         FROM story_pairs AS p JOIN t AS a ON a.key = p.a JOIN t AS b ON b.key = p.b) AS p
# MAGIC )
# MAGIC SELECT i.key_a, i.key_b, 'incident id' AS rule, s.similarity, 'same crash' AS status
# MAGIC FROM id_pairs AS i LEFT JOIN story AS s USING (key_a, key_b)
# MAGIC UNION ALL
# MAGIC SELECT s.key_a, s.key_b,
# MAGIC   CASE WHEN gap IS NULL THEN 'story, no time' WHEN least(gap, 1440 - gap) = 0 THEN 'story, same minute'
# MAGIC        WHEN least(gap, 1440 - gap) <= 10 THEN 'story, 1-10 min apart' ELSE 'story, over 10 min apart' END,
# MAGIC   s.similarity,
# MAGIC   CASE WHEN gap IS NULL THEN 'to check' WHEN least(gap, 1440 - gap) = 0 THEN 'same crash'
# MAGIC        WHEN least(gap, 1440 - gap) <= 10 THEN 'to check' ELSE 'different' END
# MAGIC FROM story AS s LEFT ANTI JOIN id_pairs AS i USING (key_a, key_b)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 4. The links table: one row per pair. Created on the first run; later runs keep it.
# MAGIC CREATE TABLE IF NOT EXISTS workspace.triage.links (
# MAGIC   key_a STRING, key_b STRING, rule STRING, similarity DOUBLE, status STRING, decided_by STRING, found_at TIMESTAMP
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 5. New pairs come in decided by the rule; a triager's decision is never overwritten; a rule pair that is gone is removed
# MAGIC MERGE INTO workspace.triage.links AS t
# MAGIC USING found AS s
# MAGIC ON t.key_a = s.key_a AND t.key_b = s.key_b
# MAGIC WHEN MATCHED AND t.decided_by = 'rule' AND NOT (t.rule <=> s.rule AND t.status <=> s.status AND t.similarity <=> s.similarity)
# MAGIC   THEN UPDATE SET rule = s.rule, similarity = s.similarity, status = s.status
# MAGIC WHEN NOT MATCHED THEN INSERT (key_a, key_b, rule, similarity, status, decided_by, found_at)
# MAGIC   VALUES (s.key_a, s.key_b, s.rule, s.similarity, s.status, 'rule', current_timestamp())
# MAGIC WHEN NOT MATCHED BY SOURCE AND t.decided_by = 'rule' THEN DELETE

# COMMAND ----------

# 6. Crashes: tickets joined by 'same crash' links form one crash, named by its lowest DAT key
same = spark.table("workspace.triage.links").filter("status = 'same crash'").select("key_a", "key_b").collect()
parent = {}


def crash_of(key: str) -> str:
    """Follow the links up to the crash's first ticket."""
    while parent.get(key, key) != key:
        key = parent[key]
    return key


def number(key: str) -> int:
    return int(key.split("-")[1])  # DAT-1360 -> 1360, so DAT-9 comes before DAT-10


for a, b in same:
    ca, cb = crash_of(a), crash_of(b)
    if ca != cb:
        parent[max(ca, cb, key=number)] = min(ca, cb, key=number)  # the higher crash joins the lower one

keys = [r.key for r in tickets.select("key").collect()]
crashes = spark.createDataFrame([(k, crash_of(k)) for k in keys], schema="key STRING, crash_key STRING")
crashes.write.mode("overwrite").saveAsTable("workspace.triage.crashes")  # rebuilt every run from the links
print(f"{len(keys):,} tickets -> {len(set(crash_of(k) for k in keys)):,} crashes")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 7. Checks: if one fails, the cell stops with the message and the run counts as failed
# MAGIC SELECT
# MAGIC   assert_true(count(*) = count(DISTINCT l.key_a, l.key_b), 'a pair appears twice') AS one_row_per_pair,
# MAGIC   assert_true(count_if(a.key IS NULL OR b.key IS NULL) = 0, 'a link names an exact copy or a missing ticket') AS only_real_tickets,
# MAGIC   assert_true(count_if(l.rule = 'incident id' AND a.incident_id <> b.incident_id) = 0, 'an incident id link joins two ids') AS id_links_share_the_id,
# MAGIC   assert_true(count_if(l.rule <> 'incident id' AND (a.month <> b.month OR a.city <> b.city)) = 0, 'a story pair is not from the same month and city') AS story_links_same_month_city,
# MAGIC   assert_true(count_if(l.status NOT IN ('same crash', 'to check', 'different')) = 0, 'a link has an unknown status') AS known_status,
# MAGIC   assert_true((SELECT count(*) FROM workspace.triage.crashes) = (SELECT count(*) FROM t), 'a ticket has no crash') AS every_ticket_has_a_crash
# MAGIC FROM workspace.triage.links AS l
# MAGIC LEFT JOIN t AS a ON a.key = l.key_a
# MAGIC LEFT JOIN t AS b ON b.key = l.key_b

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 8. The result: pairs by rule and status
# MAGIC SELECT rule, status, count(*) AS pairs FROM workspace.triage.links GROUP BY rule, status ORDER BY pairs DESC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 9. Tickets and crashes: the numbers the reports use (with and without the links)
# MAGIC SELECT count(*) AS tickets, count(DISTINCT crash_key) AS crashes, count(*) - count(DISTINCT crash_key) AS linked_away
# MAGIC FROM workspace.triage.crashes