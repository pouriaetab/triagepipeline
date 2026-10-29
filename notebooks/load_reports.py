# Databricks notebook source
# MAGIC %md
# MAGIC # load_reports: step3 of the triage pipeline
# MAGIC Reads the two NHTSA files that passed step2 (copied to the volume by `scripts/upload.sh`)
# MAGIC and keeps one row per report in the Delta table `workspace.triage.reports`:
# MAGIC - a new report is **inserted**
# MAGIC - a report whose contents changed is **updated**
# MAGIC - an unchanged report is **left alone**, so running this twice changes nothing
# MAGIC
# MAGIC The table records every change (Change Data Feed), so step 4 can read only the new or changed reports.

# COMMAND ----------
# 1. Read both files: our column names, a fingerprint of each whole row, the newest version of each report
from pyspark.sql import Window
from pyspark.sql import functions as F

VOLUME = "/Volumes/workspace/triage/nhtsa"
TABLE = "workspace.triage.reports"

def read_file(name: str):
    """One NHTSA file as a table: the six columns we use, a row fingerprint, the file name, the load time."""
    df = spark.read.csv(f"{VOLUME}/{name}", header=True, multiLine=True, escape='"') # stories hold line breaks and "" quotes
    return df.select(
        F.col("`Report ID`").alias("report_id"),
        F.col("`Report Version`").cast("int").alias("report_version"),
        F.col("`Reporting Entity`").alias("company"),
        F.col("`Incident Date`").alias("incident_month"),
        F.col("City").alias("city"),
        F.col("Narrative").alias("narrative"),
        F.expr("sha2(to_json(struct(*)), 256)").alias("row_hash"), # a change anywhere in the row changes the hash
        F.lit(name).alias("source_file"),
        F.current_timestamp().alias("loaded_at")
    )
newest_first = Window.partitionBy("report_id").orderBy(F.desc("report_version"))
incoming = (
    read_file("ads_old.csv").unionByName(read_file("ads_new.csv"))
    .withColumn("n", F.row_number().over(newest_first))
    .filter("n = 1").drop("n") # NHTSA adds a row for every update: keep only the newest version
)
incoming.createOrReplaceTempView("incoming") # the SQL cells below read it by this name
print(f"{incoming.count():,} reports in the two files")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 2. The table: one row per NHTSA report. Created on the first run; later runs keep it.
# MAGIC CREATE TABLE IF NOT EXISTS workspace.triage.reports (
# MAGIC     report_id STRING, report_version INT, company STRING, incident_month STRING, city STRING,
# MAGIC     narrative STRING, row_hash STRING, source_file STRING, loaded_at TIMESTAMP
# MAGIC )   TBLPROPERTIES (delta.enableChangeDataFeed = true) -- remember every insert and update

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 3. New reports go in, changed reports are updated, unchanged ones are not touched
# MAGIC MERGE INTO workspace.triage.reports AS t
# MAGIC MERGE USING incoming AS s
# MAGIC ON t.report_id = s.report_id
# MAGIC WHEN MATCHED AND t.row_hash <> s.row_hash THEN UPDATE SET *
# MAGIC WHEN NOT MATCHED THEN INSERT *

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 4. Checks: if one fails, the cell stops with the message and the run counts as failed
# MAGIC SELECT
# MAGIC     assert_true(count(*) = count(DISTINCT report_id), 'a report appears twice in the table') AS one_row_per_report, 
# MAGIC     assert_true(count(*) >= (SELECT count(*) FROM incoming), 'a report in the files is missing') AS nothing_missing
# MAGIC FROM workspace.triage.reports

# COMMAND ----------

# 5. What this run changed: the reports step 4 will turn into tickets
last = spark.sql(f"DESCRIBE HISTORY {TABLE} LIMIT 1").first()
m = last["operationMetrics"]
print(f"version {last['version']} ({last['operation']}): "
      f"{m.get('numTargetRowsInserted', 0)} new, {m.get('numTargetRowsUpdated', 0)} changed")
display(spark.sql(f"""
    SELECT _change_type, report_id, report_version, company, incident_month, city
    FROM table_changes('{TABLE}', {last['version']})
    WHERE _change_type IN ('insert', 'update_postimage')"""))