# Databricks notebook source
# MAGIC %md
# MAGIC # build_tickets: step 4 of the triage pipeline
# MAGIC Turns the reports that step 3 inserted or changed into tickets in `workspace.triage.tickets`:
# MAGIC - every report gets a key **DAT-n** once and keeps it for life (`workspace.triage.ticket_keys`)
# MAGIC - every ticket gets a severity **S0 to S3** from the injury field; a person outside a car or an airbag raises S0 to S1
# MAGIC - a value not in `config/known_values.json` never stops the run: its ticket gets at least S1 and `severity_pending`,
# MAGIC and once a person adds the value to the list, the next run recomputes it
# MAGIC - a report that is an **exact copy** of an earlier one (same story word for word, same month, city and minute) points to the ticket it copies
# MAGIC
# MAGIC It read only what changed since its last run (the Change Data Feed of `workspace.triage.reports`)
# MAGIC and remembers how far it got in `workspace.triage.pipeline_state`.


# COMMAND ----------

# MAGIC %sql
# MAGIC -- 1. The three tables this step owns. Created on the first run; later runs keep them.
# MAGIC CREATE TABLE IF NOT EXISTS workspace.triage.pipeline_state (step STRING, last_version BIGINT);
# MAGIC CREATE TABLE IF NOT EXISTS workspace.triage.ticket_keys (report_id STRING, key_n INT);
# MAGIC CREATE TABLE IF NOT EXISTS workspace.triage.tickets (
# MAGIC     key STRING, report_id STRING, company STRING, month STRING, city STRING, injury STRING,
# MAGIC     severity INT, severity_pending BOOLEAN, copy_of STRING, story STRING, updated_at TIMESTAMP
# MAGIC )   TBLPROPERTIES (delta.enableChangeDataFeed = true)

# COMMAND ----------

# 1b. A tickets table made before severity_pending existed gets the column once (every value in it was known then)
TICKETS = "workspace.triage.tickets"
if "severity_pending" not in spark.table(TICKETS).columns:
    spark.sql(f"ALTER TABLE {TICKETS} ADD COLUMNS (severity_pending BOOLEAN)")
    spark.sql(f"UPDATE {TICKETS} SET severity_pending = false")

# COMMAND ----------

# 2. What changed in the reports table since this step last ran
import json
from pathlib import Path

from pyspark.sql import Window
from pyspark.sql import functions as F

REPORTS = "workspace.triage.reports"
STATE = "workspace.triage.pipeline_state"
STEP = "build_tickets"

current = spark.sql(f"DESCRIBE HISTORY {REPORTS} LIMIT 1").first()["version"]  # newest version of the reports table
done = spark.table(STATE).filter(F.col("step") == STEP).agg(F.max("last_version")).first()[0]  # None on the first run
start = 0 if done is None else done + 1 # first run: read the table's whole history

newest_change = Window.partitionBy("report_id").orderBy(F.desc("_commit_version"))
if start <= current:
    changed = (
        spark.sql(f"SELECT * FROM table_changes('{REPORTS}', {start}, {current})")
        .filter(F.col("_change_type").isin("insert", "update_postimage"))  # the rows as they are now, not as they were
        .withColumn("n", F.row_number().over(newest_change))
        .filter("n = 1")  # a report changed twice since the last run: keep its latest state
        .drop("n", "_change_type", "_commit_version", "_commit_timestamp")
    )
else:
    changed = spark.table(REPORTS).limit(0)  # no new version of the reports table since the last run
# a ticket still waiting for a person is built again every run, so a value added to the list fixes it on the next run
waiting = spark.sql(f"SELECT r.* FROM {REPORTS} AS r JOIN {TICKETS} AS t using (report_id) WHERE t.severity_pending")
changed = changed.unionByName(waiting.join(changed.select("report_id"), "report_id", "left_anti"))
if changed.isEmpty():
    dbutils.notebook.exit(f"nothing new: reports is at version {current} and no ticket is waiting")
changed.createOrReplaceTempView("changed")
print(f"reports table at version {current}: {changed.count():,} new, changed or waiting reports")

# COMMAND ----------

# 3. A key for every report that has none yet: the next numbers, oldest month first, then by report id
KEYS = "workspace.triage.ticket_keys"
MONTH = F.expr("coalesce(date_format(try_to_timestamp(initcap(incident_month), 'MMM-yyyy'), 'yyyy-MM'), 'unknown')") # JUN-2025 -> 2025-06

highest = spark.table(KEYS).agg(F.max("key_n")).first()[0] or 0 # the highest key so far, 0 on the first run
new_keys = (
    changed.join(spark.table(KEYS), "report_id", "left_anti") # only reports that have no key yet
    .withColumn("month", MONTH)
    .withColumn("key_n", F.lit(highest) + F.row_number().over(Window.orderBy("month", "report_id")))
    .select("report_id", "key_n")
)
n_new = new_keys.count() # count before writing: afterwards these reports already have keys
new_keys.write.mode("append").saveAsTable(KEYS) # a key is only ever added, never changed
print(f"{n_new:,} new keys, from DAT-{highest + 1}")

# COMMAND ----------

# 4. The ticket fields, the same rules for the changed reports and for all reports
MONTHS = "January|February|March|April|May|June|July|August|September|October|November|December"
SEVERITY = """
  CASE
    WHEN injury LIKE 'Fatality%' OR injury LIKE 'Serious%' THEN 3
    WHEN injury LIKE 'Moderate%' OR injury = 'Minor W/ Hospitalization' THEN 2
    WHEN injury LIKE 'Minor%' OR injury = 'Unknown' OR injury IS NULL THEN 1    -- unknown is never S0
    WHEN crash_with LIKE 'Non-Motorist%' OR crash_with = 'Motorcycle' THEN 1    -- a person outside a car
    WHEN airbag LIKE 'Yes Subject%' OR airbag = 'Yes' THEN 1                    -- the AV's airbag went off
    ELSE 0
  END"""

# the values a person has checked (the same list step 2 uses); any other value is pending
KNOWN = json.loads(Path("../config/known_values.json").read_text())  # the notebook runs in notebooks/, the list is in config/


def known(column: str, values: list[str]) -> str:
    """SQL that is true when the column is empty or holds one of the checked values."""
    quoted = ", ".join("'" + v.replace("'", "''") + "'" for v in values)  # a ' inside a value is written ''
    return f"coalesce(trim({column}), '') IN ('', {quoted})"


INJURIES = KNOWN["Highest Injury Severity Alleged"]
CRASH_WITH = KNOWN["Crash With"]
AIRBAGS = KNOWN["SV Any Air Bags Deployed?"] + KNOWN["Any Air Bags Deployed?"]  # the old and the new form's column
PENDING = f"NOT ({known('injury', INJURIES)} AND {known('crash_with', CRASH_WITH)} AND {known('airbag', AIRBAGS)})"
SAFE_SEVERITY = f"CASE WHEN {PENDING} THEN greatest({SEVERITY}, 1) ELSE {SEVERITY} END"  # IMPORTANT: pending is never S0
keys = spark.table(KEYS)


def with_fields(df):
    """Adds the month (2025-06), the clock time (05:21), the severity, and a signature that is
    the same only for exact copies: the story word for word, month, city and minute."""
    story = F.regexp_replace(F.coalesce("narrative", F.lit("")), rf"(?s)^.*?(?=\bOn ({MONTHS}) )", "") # drop an update note before the story
    story = F.trim(F.regexp_replace(F.regexp_replace(F.lower(story), r"\s+", " "), "[^a-z0-9 ]", "")) # lower case, letters and digits only
    clock = F.when(F.col("incident_time").rlike(r"^\s*\d{1,2}:\d{2}"),
                   F.format_string("%02d:%s", F.regexp_extract("incident_time", r"^\s*(\d{1,2}):", 1).cast("int"), 
                                   F.regexp_extract("incident_time", r":(\d{2})", 1))).otherwise("")
    sig = F.when(F.length(story) >= 100, F.concat_ws("|", story, MONTH, F.lower(F.trim("city")), clock)) # short or redacted stories never match
    return df.join(keys, "report_id").select("*", MONTH.alias("month"), F.expr(SAFE_SEVERITY).alias("severity"),
                                             F.expr(PENDING).alias("severity_pending"), sig.alias("sig"))


mine = with_fields(spark.table("changed"))
everyone = with_fields(spark.table(REPORTS)).select("sig", F.col("key_n").alias("other_n"))
copies = (
    mine.join(everyone, "sig") # same story, month, city and minute
    .filter("other_n < key_n") # the earlier report is the one that stays
    .groupBy("report_id").agg(F.min("other_n").alias("copy_n"))
)
tickets = mine.join(copies, "report_id", "left").select(
    F.concat(F.lit("DAT-"), "key_n").alias("key"),
    "report_id",
    "company",
    "month",
    F.initcap(F.trim("city")).alias("city"),
    F.coalesce("injury", F.lit("Unknown")).alias("injury"),
    "severity",
    "severity_pending",
    F.when(F.col("copy_n").isNotNull(), F.concat(F.lit("DAT-"), "copy_n")).alias("copy_of"),
    F.col("narrative").alias("story"),
    F.current_timestamp().alias("updated_at"),
)
tickets.createOrReplaceTempView("new_tickets")
print(f"{tickets.count():,} tickets to write, {tickets.filter('copy_of IS NOT NULL').count()} of them exact copies")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 5. Write the tickets: a new report becomes a new ticket, a changed report updates its ticket
# MAGIC MERGE INTO workspace.triage.tickets AS t
# MAGIC USING new_tickets AS s
# MAGIC ON t.report_id = s.report_id
# MAGIC WHEN MATCHED THEN UPDATE SET *
# MAGIC WHEN NOT MATCHED THEN INSERT *

# COMMAND ----------

# 6. Save the bookmark: the reports version this run has now handled
# IMPORTANT: keep this the last write. If a cell above fails, the bookmark stays where it was,
#            so the next run handles the same changes again; the MERGEs make that safe.
spark.sql(f"""
    MERGE INTO {STATE} AS s
    USING (SELECT '{STEP}' AS step, {current} AS last_version) AS n
    ON s.step = n.step
    WHEN MATCHED THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *""")
print(f"{STEP} has read the reports table up to version {current}")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 7. Checks: if one fails, the cell stops with the message and the run counts as failed
# MAGIC SELECT
# MAGIC     assert_true(count(*) = count(DISTINCT report_id), 'a report has two tickets') AS one_ticket_per_report,
# MAGIC     assert_true(count(*) = (SELECT count(*) FROM workspace.triage.reports), 'a report has no ticket') AS every_report_has_one,
# MAGIC     assert_true(count(DISTINCT key) = count(*), 'two tickets share a key') AS keys_unique,
# MAGIC     assert_true(count_if(severity_pending AND severity = 0) = 0, 'a pending ticket is S0') AS pending_never_s0
# MAGIC FROM workspace.triage.tickets

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 8. The result: tickets by severity; exact copies are counted apart, not as tickets
# MAGIC SELECT
# MAGIC     count_if(copy_of IS NULL) AS tickets,
# MAGIC     count_if(copy_of IS NOT NULL) AS exact_copies,
# MAGIC     count_if(copy_of IS NULL AND severity = 3) AS s3,
# MAGIC     count_if(copy_of is NULL AND severity = 2) AS s2,
# MAGIC     count_if(copy_of is NULL AND severity = 1) AS s1,
# MAGIC     count_if(copy_of IS NULL AND severity = 0) AS s0
# MAGIC FROM workspace.triage.tickets

# COMMAND ----------

# 9. The review list: one row per value a person has not checked yet, however many tickets carry it
review = spark.sql(f"""
    SELECT field, value, count(*) AS tickets, slice(sort_array(collect_list(key)), 1, 3) AS examples
    FROM (SELECT t.key, r.injury, r.crash_with, r.airbag
          FROM {TICKETS} AS t JOIN {REPORTS} AS r USING (report_id) WHERE t.severity_pending)
    LATERAL VIEW stack(3, 'injury', injury, 'crash_with', crash_with, 'airbag', airbag) AS field, value
    WHERE NOT CASE field WHEN 'injury' THEN {known('value', INJURIES)}
                         WHEN 'crash_with' THEN {known('value', CRASH_WITH)}
                         ELSE {known('value', AIRBAGS)} END
    GROUP BY field, value
    ORDER BY tickets DESC""")
display(review)