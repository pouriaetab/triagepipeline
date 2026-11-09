# Databricks notebook source
# MAGIC %md
# MAGIC # spike_report: step 6 of the triage pipeline, on the crashes
# MAGIC Runs the spike detector (`find_spikes` in `scripts/spikes.py`, the same code and tests) on what the AV hit,
# MAGIC once counting tickets and once counting crashes (step 5 links the same crash filed twice), and keeps both in
# MAGIC `workspace.triage.spikes`:
# MAGIC - a **spike** is a month where a value's share is above its upper limit from the 12 months before (3 sigmas)
# MAGIC - a **new value** is one never seen in the 12 months before: a person checks it for a form change first
# MAGIC - every share has its 95% range (Wilson)
# MAGIC - a spike that shows only when counting tickets came from the same crash filed twice

# COMMAND ----------

# 1. One row per ticket and one row per crash (the crash's first ticket), with the month and what the AV hit
import os
import sys

import pandas as pd

sys.path.append(os.path.abspath("../scripts"))  # the repo's scripts folder, next to this notebooks folder
from spikes import find_spikes  # noqa: E402

rows = spark.sql("""
    SELECT t.key, c.crash_key, t.month, r.crash_with
    FROM workspace.triage.tickets AS t
    JOIN workspace.triage.crashes AS c USING (key)
    JOIN workspace.triage.reports AS r USING (report_id)""").toPandas()
tickets = rows
crashes = rows[rows["key"] == rows["crash_key"]]  # the first ticket of each crash stands for it
print(f"{len(tickets):,} tickets, {len(crashes):,} crashes")

# COMMAND ----------

# 2. The spikes, counted both ways, in one table
found = pd.concat([find_spikes(tickets, "month", "crash_with").assign(counted_by="tickets"),
                   find_spikes(crashes, "month", "crash_with").assign(counted_by="crashes")], ignore_index=True)
spark.createDataFrame(found).write.mode("overwrite").saveAsTable("workspace.triage.spikes")  # rebuilt every run
for by in ["tickets", "crashes"]:
    f = found[found.counted_by == by]
    print(f"counted by {by}: {(f.status == 'spike').sum()} spikes, {(f.status == 'new value').sum()} new values")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 3. Checks: if one fails, the cell stops with the message and the run counts as failed
# MAGIC SELECT
# MAGIC   assert_true(count_if(share < 0 OR share > 1) = 0, 'a share outside 0 to 1') AS shares_between_0_and_1,
# MAGIC   assert_true(count_if(low > share OR share > high) = 0, 'a share outside its 95% range') AS share_inside_its_range,
# MAGIC   assert_true(count_if(status = 'spike' AND share <= upper) = 0, 'a spike below its limit') AS spikes_above_the_limit,
# MAGIC   assert_true(count_if(rows < 3) = 0, 'a flag on fewer than 3 rows') AS at_least_3_rows,
# MAGIC   assert_true(count_if(counted_by NOT IN ('tickets', 'crashes')) = 0, 'an unknown count') AS known_count
# MAGIC FROM workspace.triage.spikes

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 4. The result: the spikes counted by crashes, newest first, and whether counting tickets finds them too
# MAGIC SELECT c.month, c.value, c.rows AS crashes, c.month_total, c.share, c.normal, c.upper, c.low, c.high, c.status,
# MAGIC   t.month IS NOT NULL AS also_by_tickets
# MAGIC FROM workspace.triage.spikes AS c
# MAGIC LEFT JOIN workspace.triage.spikes AS t ON t.counted_by = 'tickets' AND t.month = c.month AND t.value = c.value
# MAGIC WHERE c.counted_by = 'crashes'
# MAGIC ORDER BY c.month DESC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 5. Spikes that show only when counting tickets: the same crash filed twice made them
# MAGIC SELECT t.month, t.value, t.rows AS tickets, t.share
# MAGIC FROM workspace.triage.spikes AS t
# MAGIC LEFT ANTI JOIN workspace.triage.spikes AS c ON c.counted_by = 'crashes' AND c.month = t.month AND c.value = t.value
# MAGIC WHERE t.counted_by = 'tickets'
# MAGIC ORDER BY t.month DESC