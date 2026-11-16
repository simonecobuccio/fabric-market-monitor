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

# The pipeline overwrites this value with its own run id (pipeline().RunId).
# When the notebook is run by hand it stays empty and we create an id in the next cell.
run_id = ""

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Setup

# CELL ********************

import uuid
import json
from datetime import datetime, timedelta, timezone
from pyspark.sql.types import (
    StructType, StructField, StringType, TimestampType, BooleanType, LongType
)

# If no run id came from the pipeline, this is a manual run: create one.
if run_id is None or run_id == "":
    run_id = str(uuid.uuid4())
    run_mode = "manual"
else:
    run_mode = "pipeline"

# Same convention as silver: UTC time without timezone information
run_ts = datetime.now(timezone.utc).replace(tzinfo=None)
run_date = run_ts.date()

DQ_TABLE = "gold.dq_results"

# Tolerance for freshness: Swiss and Italian holidays are different,
# so "last business day" would give false alarms (documented limit in the README).
FRESHNESS_MAX_BUSINESS_DAYS = 3

print(f"run_id: {run_id} ({run_mode})")
print(f"run_ts: {run_ts}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Helpers

# CELL ********************

def table_exists(table_name):
    # spark.table() fails immediately if the table does not exist,
    # so we use it as a simple existence test.
    try:
        spark.table(table_name)
        return True
    except Exception:
        return False


def business_days_back(start_date, number_of_days):
    # Go back one day at a time and count only Monday to Friday.
    # Example: Saturday 14/11, 3 days -> Fri 13 (1), Thu 12 (2), Wed 11 (3) -> Wed 11
    current_day = start_date
    counted_days = 0
    while counted_days < number_of_days:
        current_day = current_day - timedelta(days=1)
        if current_day.weekday() < 5:          # 0 = Monday ... 4 = Friday
            counted_days = counted_days + 1
    return current_day


def get_last_dq_run_ts():
    # Time of the previous data quality run (None if this is the first one)
    if table_exists(DQ_TABLE) == False:
        return None
    first_row = spark.sql(f"SELECT MAX(run_ts) AS last_run_ts FROM {DQ_TABLE}").collect()[0]
    return first_row["last_run_ts"]


freshness_cutoff = business_days_back(run_date, FRESHNESS_MAX_BUSINESS_DAYS)
last_dq_run_ts = get_last_dq_run_ts()

# Filter for "rejected rows that are new since the last check".
# First run: there is no previous run, so every rejected row counts as new.
if last_dq_run_ts is None:
    new_rows_filter = "1 = 1"
else:
    new_rows_filter = f"_ingested_at > TIMESTAMP '{last_dq_run_ts}'"

print(f"freshness cutoff: {freshness_cutoff}")
print(f"last dq run: {last_dq_run_ts}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Check List

# CELL ********************

# Every check is a SQL query that returns the BAD rows. 0 rows = passed.
# To add a check: add one entry to this list, nothing else changes.
# optional_table = True -> the table may not exist yet (rejected tables are created at the first reject).
checks = [
    # ---------- UNIQUENESS ----------
    {
        "check_name": "silver_prices_unique_key",
        "layer": "silver", "table_name": "silver.prices", "dimension": "uniqueness",
        "optional_table": False,
        "sql": """
            SELECT ticker, date, COUNT(*) AS row_count
            FROM silver.prices
            GROUP BY ticker, date
            HAVING COUNT(*) > 1
        """,
    },
    {
        "check_name": "silver_fx_unique_key",
        "layer": "silver", "table_name": "silver.fx_rates", "dimension": "uniqueness",
        "optional_table": False,
        "sql": """
            SELECT currency, date, COUNT(*) AS row_count
            FROM silver.fx_rates
            GROUP BY currency, date
            HAVING COUNT(*) > 1
        """,
    },
    {
        "check_name": "fact_grain_unique",
        "layer": "gold", "table_name": "gold.fact_daily_price", "dimension": "uniqueness",
        "optional_table": False,
        "sql": """
            SELECT ticker, date, COUNT(*) AS row_count
            FROM gold.fact_daily_price
            GROUP BY ticker, date
            HAVING COUNT(*) > 1
        """,
    },
    {
        "check_name": "snapshot_one_row_per_ticker",
        "layer": "gold", "table_name": "gold.latest_snapshot", "dimension": "uniqueness",
        "optional_table": False,
        "sql": """
            SELECT ticker, COUNT(*) AS row_count
            FROM gold.latest_snapshot
            GROUP BY ticker
            HAVING COUNT(*) > 1
        """,
    },
    # ---------- COMPLETENESS ----------
    {
        "check_name": "fact_complete_vs_silver",
        "layer": "gold", "table_name": "gold.fact_daily_price", "dimension": "completeness",
        "optional_table": False,
        "sql": """
            SELECT silver_prices.ticker, silver_prices.date
            FROM silver.prices AS silver_prices
            LEFT ANTI JOIN gold.fact_daily_price AS fact
                ON silver_prices.ticker = fact.ticker
               AND silver_prices.date = fact.date
        """,
    },
    {
        "check_name": "configured_ticker_has_prices",
        "layer": "silver", "table_name": "silver.tickers", "dimension": "completeness",
        "optional_table": False,
        "sql": """
            SELECT tickers.symbol
            FROM silver.tickers AS tickers
            LEFT ANTI JOIN silver.prices AS prices
                ON tickers.symbol = prices.ticker
        """,
    },
    # ---------- VALIDITY ----------
    {
        "check_name": "fact_close_chf_positive",
        "layer": "gold", "table_name": "gold.fact_daily_price", "dimension": "validity",
        "optional_table": False,
        "sql": """
            SELECT ticker, date, close_chf
            FROM gold.fact_daily_price
            WHERE close_chf <= 0
        """,
    },
    {
        "check_name": "new_rejected_prices",
        "layer": "silver", "table_name": "silver.prices_rejected", "dimension": "validity",
        "optional_table": True,
        "sql": f"""
            SELECT *
            FROM silver.prices_rejected
            WHERE {new_rows_filter}
        """,
    },
    {
        "check_name": "new_rejected_fx",
        "layer": "silver", "table_name": "silver.fx_rates_rejected", "dimension": "validity",
        "optional_table": True,
        "sql": f"""
            SELECT *
            FROM silver.fx_rates_rejected
            WHERE {new_rows_filter}
        """,
    },
    # ---------- CONSISTENCY (referential integrity) ----------
    {
        "check_name": "fact_date_in_dim_date",
        "layer": "gold", "table_name": "gold.fact_daily_price", "dimension": "consistency",
        "optional_table": False,
        "sql": """
            SELECT fact.ticker, fact.date, fact.date_key
            FROM gold.fact_daily_price AS fact
            LEFT ANTI JOIN gold.dim_date AS dim_date
                ON fact.date_key = dim_date.date_key
        """,
    },
    {
        "check_name": "fact_ticker_in_dim_ticker",
        "layer": "gold", "table_name": "gold.fact_daily_price", "dimension": "consistency",
        "optional_table": False,
        "sql": """
            SELECT fact.ticker, fact.date
            FROM gold.fact_daily_price AS fact
            LEFT ANTI JOIN gold.dim_ticker AS dim_ticker
                ON fact.ticker = dim_ticker.ticker
        """,
    },
    {
        "check_name": "fact_fx_rate_present",
        "layer": "gold", "table_name": "gold.fact_daily_price", "dimension": "consistency",
        "optional_table": False,
        "sql": """
            SELECT ticker, date, currency
            FROM gold.fact_daily_price
            WHERE rate_to_chf IS NULL
        """,
    },
    # ---------- TIMELINESS (freshness) ----------
    {
        "check_name": "prices_freshness",
        "layer": "silver", "table_name": "silver.prices", "dimension": "timeliness",
        "optional_table": False,
        "sql": f"""
            SELECT ticker, MAX(date) AS last_date
            FROM silver.prices
            GROUP BY ticker
            HAVING MAX(date) < DATE '{freshness_cutoff}'
        """,
    },
    {
        "check_name": "fx_freshness",
        "layer": "silver", "table_name": "silver.fx_rates", "dimension": "timeliness",
        "optional_table": False,
        "sql": f"""
            SELECT currency, MAX(date) AS last_date
            FROM silver.fx_rates
            GROUP BY currency
            HAVING MAX(date) < DATE '{freshness_cutoff}'
        """,
    },
]

print(f"{len(checks)} checks configured")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Run Checks

# CELL ********************

check_results = []

for check in checks:
    check_name = check["check_name"]
    status = ""
    failed_rows = None
    details = ""

    if check["optional_table"] == True and table_exists(check["table_name"]) == False:
        # The rejected table is created only at the first reject: missing = nothing was ever rejected
        status = "passed"
        failed_rows = 0
        details = "table not created yet: no rows ever rejected"
    else:
        try:
            bad_rows_df = spark.sql(check["sql"])
            failed_rows = bad_rows_df.count()

            if failed_rows == 0:
                status = "passed"
            else:
                status = "failed"
                # Keep up to 5 example rows to understand the problem later
                example_rows = bad_rows_df.limit(5).collect()
                example_texts = []
                for row in example_rows:
                    example_texts.append(str(row.asDict()))
                details = " | ".join(example_texts)
        except Exception as error:
            # A broken check must not stop the other checks
            status = "error"
            failed_rows = None
            details = str(error)[:500]

    result = {
        "run_id": run_id,
        "run_ts": run_ts,
        "run_mode": run_mode,
        "layer": check["layer"],
        "table_name": check["table_name"],
        "check_name": check_name,
        "dimension": check["dimension"],
        "status": status,
        "passed": status == "passed",
        "failed_rows": failed_rows,
        "details": details[:1000],
    }
    check_results.append(result)

    print(f"{status.upper():7} | {check['dimension']:12} | {check_name} | failed rows: {failed_rows}")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Save Results

# CELL ********************

# Fixed schema: same idea as the schema contract in bronze
dq_schema = StructType([
    StructField("run_id", StringType(), False),
    StructField("run_ts", TimestampType(), False),
    StructField("run_mode", StringType(), False),
    StructField("layer", StringType(), False),
    StructField("table_name", StringType(), False),
    StructField("check_name", StringType(), False),
    StructField("dimension", StringType(), False),
    StructField("status", StringType(), False),
    StructField("passed", BooleanType(), False),
    StructField("failed_rows", LongType(), True),     # null when the check had an error
    StructField("details", StringType(), True),
])

rows_to_save = []
for result in check_results:
    rows_to_save.append((
        result["run_id"],
        result["run_ts"],
        result["run_mode"],
        result["layer"],
        result["table_name"],
        result["check_name"],
        result["dimension"],
        result["status"],
        result["passed"],
        result["failed_rows"],
        result["details"],
    ))

results_df = spark.createDataFrame(rows_to_save, schema=dq_schema)

# APPEND, not overwrite: dq_results is a history log
results_df.write.format("delta").mode("append").saveAsTable(DQ_TABLE)

print(f"{results_df.count()} results appended to {DQ_TABLE}")
display(results_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Summary + Exit

# CELL ********************

not_passed_checks = []
for result in check_results:
    if result["status"] != "passed":
        not_passed_checks.append(result["check_name"])

if len(not_passed_checks) == 0:
    overall_status = "ok"
else:
    overall_status = "warning"     # we signal, we do not block the pipeline (decision 14/11)

summary = {
    "run_id": run_id,
    "overall_status": overall_status,
    "total_checks": len(check_results),
    "not_passed": len(not_passed_checks),
    "not_passed_checks": not_passed_checks,
}

print(json.dumps(summary, indent=2))

# The pipeline reads this value as the notebook output
notebookutils.notebook.exit(json.dumps(summary))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
