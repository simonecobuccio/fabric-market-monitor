# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "348a25fc-a849-4af4-a396-b7aac69577a7",
# META       "default_lakehouse_name": "lh_market",
# META       "default_lakehouse_workspace_id": "e97c361b-917f-4d21-8db4-5bad63e77e4f",
# META       "known_lakehouses": [
# META         {
# META           "id": "348a25fc-a849-4af4-a396-b7aac69577a7"
# META         }
# META       ]
# META     },
# META     "environment": {
# META       "environmentId": "6905f647-6240-ab96-4a4a-f437fb11d2bd",
# META       "workspaceId": "00000000-0000-0000-0000-000000000000"
# META     }
# META   }
# META }

# MARKDOWN ********************

# # Parameters

# PARAMETERS CELL ********************

# All values are strings: the pipeline overwrites them for every run.
run_id = ""                         # @pipeline().RunId
pipeline_name = "pl_daily_market"
trigger_time = ""                   # @pipeline().TriggerTime
status = ""                         # "succeeded" or "failed"
failed_step = ""                    # "bronze", "silver", "gold" or "dq" (only when failed)
error_message = ""                  # @activity('<step>').error.message (only when failed)
dq_status = ""                      # "ok" or "warning" (only when succeeded)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Setup + validation

# CELL ********************

import uuid
from datetime import datetime, timezone
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, LongType


def empty_to_none(value):
    # Store empty parameters as NULL, not as empty strings
    if value is None or value == "":
        return None
    return value


# Manual test run: no run id from the pipeline
if run_id is None or run_id == "":
    run_id = str(uuid.uuid4())
    run_mode = "manual"
else:
    run_mode = "pipeline"

# Same convention as the other notebooks: UTC without timezone information
end_ts = datetime.now(timezone.utc).replace(tzinfo=None)

# TriggerTime looks like 2026-11-15T12:00:00.1234567Z (7 decimals, Python handles 6):
# we keep only the first 19 characters, precision to the second is enough.
if trigger_time is None or trigger_time == "":
    trigger_ts = end_ts
else:
    trigger_ts = datetime.strptime(trigger_time[:19], "%Y-%m-%dT%H:%M:%S")

duration_seconds = int((end_ts - trigger_ts).total_seconds())

# The pipeline only says "succeeded": the data quality result decides if there are warnings
if status == "succeeded" and dq_status == "warning":
    status = "succeeded_with_warnings"

allowed_statuses = ["succeeded", "succeeded_with_warnings", "failed"]
if status not in allowed_statuses:
    raise ValueError(f"Unexpected status '{status}'. Allowed values: {allowed_statuses}")

print(f"run_id: {run_id} ({run_mode})")
print(f"status: {status} | duration: {duration_seconds} s")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #  Save

# CELL ********************

RUNS_TABLE = "gold.pipeline_runs"

runs_schema = StructType([
    StructField("run_id", StringType(), False),
    StructField("pipeline_name", StringType(), False),
    StructField("run_mode", StringType(), False),
    StructField("trigger_ts", TimestampType(), False),
    StructField("end_ts", TimestampType(), False),
    StructField("duration_seconds", LongType(), False),
    StructField("status", StringType(), False),
    StructField("failed_step", StringType(), True),       # null when the run succeeded
    StructField("error_message", StringType(), True),     # null when the run succeeded
    StructField("dq_status", StringType(), True),         # null when the run failed
])

error_text = empty_to_none(error_message)
if error_text is not None:
    error_text = error_text[:1000]

run_row = (
    run_id,
    pipeline_name,
    run_mode,
    trigger_ts,
    end_ts,
    duration_seconds,
    status,
    empty_to_none(failed_step),
    error_text,
    empty_to_none(dq_status),
)

run_df = spark.createDataFrame([run_row], schema=runs_schema)

# APPEND: one row per pipeline run, it is a history log
run_df.write.format("delta").mode("append").saveAsTable(RUNS_TABLE)

print(f"1 row appended to {RUNS_TABLE}")
display(run_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Finish

# CELL ********************

# If a step failed, the run must look FAILED in Monitor.
# Without this exception Data Factory sees the error as "handled" and the run as succeeded.
if status == "failed":
    raise Exception(f"Pipeline step '{failed_step}' failed: {error_message}")

notebookutils.notebook.exit(status)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
