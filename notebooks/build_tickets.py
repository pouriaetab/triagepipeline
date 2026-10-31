# Databricks notebook source
# MAGIC %md
# MAGIC # build_tickets: step4 of the triage pipeline
# MAGIC Turns the reports that step 3 inserted or changed into tickets in `workspace.triage.tickets`:
# MAGIC - every report gets a key **DAT-n** once and keeps it for life (`workspace.triage.ticket_keys`)
# MAGIC - every ticket gets a severity **S0 to S3** from the injury field; a person outside a car or an airbag raises S0 to S1
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
# MAGIC     severity INT, copy_of STRING, story STRING, updated_at TIMESTAMP
# MAGIC )   TBLPROPERTIES (delta.enableChangeDataFeed = true)

# COMMAND ----------

# 2. What changed in the reports table since this step last ran
from pyspark.sql import Window
from pyspark.sql import functions as F

REPORTS = "workspace.triage.reports"
STATE = "workspace.triage.pipeline_state"
STEP = "build_tickets"

current = spark.sql(f"DESCRIBE HISTORY {REPORTS} LIMIT 1").first()["version"] # newest version of the reports table
done = spark.table(STATE).filter(F.col("step") == STEP).agg(F.max("last_version")).first()[0] # None on the first run
start = 0 if done is None else done + 1 # first run: read the table's whole history

if start > current:
    dbutils.notebook.exit(f"nothing new: reports is at version {current} and this step already read it")

newest_change = Window.partitionBy("report_id").orderBy(F.desc("_commit_version"))
changed = (
    spark.sql(f"SELECT * FROM table_changes('{REPORTS}', {start}, {current})")
    .filter(F.col("_change_type").isin("insert", "update_postimage")) # the rows as they are now, not as they were
    .withColumn("n", F.row_number().over(newest_change))
    .filter("n = 1") # a report changed twice since the last run: keep its latest state
    .drop("n", "_change_type", "_commit_version", "_commit_timestamp")
)
changed.createOrReplaceTempView("changed")
print(f"reports versions {start} to {current}: {changed.count():,} new or changed reports")

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
    return df.join(keys, "report_id").select("*", MONTH.alias("month"), F.expr(SEVERITY).alias("severity"), sig.alias("sig"))


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
# MAGIC     assert_true(count(DISTINCT key) = count(*), 'two tickets share a key') AS keys_unique
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