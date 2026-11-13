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

# CELL ********************

from datetime import datetime, timezone

from delta.tables import DeltaTable
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# Time of this run in UTC (no time zone info, like in the bronze notebook)
now_utc = datetime.now(timezone.utc)
RUN_TS = now_utc.replace(tzinfo=None)
RUN_DATE = RUN_TS.date()

# Bronze source tables
BRONZE_TICKERS = "bronze.tickers_raw"
BRONZE_PRICES = "bronze.prices_raw"
BRONZE_FX = "bronze.fx_raw"

# Silver target tables
SILVER_TICKERS = "silver.tickers"
SILVER_PRICES = "silver.prices"
SILVER_FX = "silver.fx_rates"

# Rejected tables (invalid rows are kept here with the reason, never deleted)
REJECTED_PRICES = "silver.prices_rejected"
REJECTED_FX = "silver.fx_rejected"

# Counters of this run, filled by cells 4 and 5 and checked in cell 6
run_stats = {}

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Support functions

# CELL ********************

def get_last_ingested(table_names):
    """Most recent _ingested_at found in the given tables, or None if none of them has data yet."""
    last = None

    for table_name in table_names:
        if not spark.catalog.tableExists(table_name):
            continue

        value = spark.table(table_name).agg(F.max("_ingested_at").alias("max_ts")).first()["max_ts"]
        if value is None:
            continue

        if last is None or value > last:
            last = value

    return last


def read_new_bronze_rows(bronze_table, silver_tables):
    """Bronze rows that arrived after the last silver run (all rows on the first run)."""
    bronze_df = spark.table(bronze_table)

    # Watermark = last ingestion time already processed (valid OR rejected rows)
    last = get_last_ingested(silver_tables)

    if last is None:
        return bronze_df

    return bronze_df.filter(F.col("_ingested_at") > F.lit(last))


def keep_latest(df, key_columns):
    """Keep one row per key: the one with the most recent _ingested_at."""
    # 1. Group the rows by key and sort each group from newest to oldest
    window = Window.partitionBy(key_columns).orderBy(F.col("_ingested_at").desc())

    # 2. Number the rows inside each group: 1 = newest
    df = df.withColumn("row_num", F.row_number().over(window))

    # 3. Keep only the newest row and remove the helper column
    df = df.filter(F.col("row_num") == 1)
    df = df.drop("row_num")

    return df


def merge_into(target_table, source_df, key_columns):
    """Upsert: update rows whose key already exists, insert the new ones."""

    # First run: the table doesn't exist yet, so we simply create it
    if not spark.catalog.tableExists(target_table):
        source_df.write.format("delta").saveAsTable(target_table)
        return

    # Build the match condition, e.g. "target.`ticker` = source.`ticker` AND target.`date` = source.`date`"
    condition_parts = []
    for key in key_columns:
        condition_parts.append("target.`" + key + "` = source.`" + key + "`")
    condition = " AND ".join(condition_parts)

    target = DeltaTable.forName(spark, target_table)

    (
        target.alias("target")
        .merge(source_df.alias("source"), condition)
        .whenMatchedUpdateAll()      # key exists -> update the row
        .whenNotMatchedInsertAll()   # key is new -> insert the row
        .execute()
    )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Ticker Table

# CELL ********************

tickers_df = spark.table(BRONZE_TICKERS)

# 1. Remove extra spaces from every text column
for column_name, column_type in tickers_df.dtypes:
    if column_type == "string":
        tickers_df = tickers_df.withColumn(column_name, F.trim(F.col(column_name)))

# 2. Symbols always in uppercase
tickers_df = tickers_df.withColumn("symbol", F.upper(F.col("symbol")))

# 3. One row per symbol
tickers_df = keep_latest(tickers_df, ["symbol"])

# 4. When silver processed it
tickers_df = tickers_df.withColumn("_processed_at", F.lit(RUN_TS))

# Reference table: only today's version matters, so we overwrite it
tickers_df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(SILVER_TICKERS)

print(SILVER_TICKERS, tickers_df.count(), "rows")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Prices

# CELL ********************

PRICE_KEYS = ["ticker", "date"]

# 1. Read only the new bronze rows
new_rows = read_new_bronze_rows(BRONZE_PRICES, [SILVER_PRICES, REJECTED_PRICES])
n_new = new_rows.count()

if n_new == 0:
    print("Prices already up to date")
    run_stats["prices"] = {"new": 0, "deduped": 0, "valid": 0, "rejected": 0}
else:
    # 2. One row per ticker + date (the newest one)
    deduped = keep_latest(new_rows, PRICE_KEYS)
    n_deduped = deduped.count()

    # 3. Tag the reject reason: rules are checked in order, the first true rule wins
    reject_reason = (
        F.when(F.col("close").isNull() | (F.col("close") <= 0), "invalid close")
        .when(F.col("high") < F.col("low"), "high < low")
        .when(F.col("date") > F.lit(RUN_DATE), "future date")
        .when(F.col("volume") < 0, "negative volume")
        # no rule true -> null = valid row
    )
    tagged = deduped.withColumn("reject_reason", reject_reason)
    tagged = tagged.withColumn("_processed_at", F.lit(RUN_TS))

    # 4. Split valid and rejected rows
    valid = tagged.filter(F.col("reject_reason").isNull()).drop("reject_reason")
    rejected = tagged.filter(F.col("reject_reason").isNotNull())

    n_valid = valid.count()
    n_rejected = rejected.count()

    # 5. Rejected rows: appended with the reason (never deleted)
    if n_rejected > 0:
        rejected.write.mode("append").saveAsTable(REJECTED_PRICES)

    # 6. Valid rows: upsert into silver
    merge_into(SILVER_PRICES, valid, PRICE_KEYS)

    run_stats["prices"] = {"new": n_new, "deduped": n_deduped, "valid": n_valid, "rejected": n_rejected}
    print("Prices -> read:", n_new, "| duplicates removed:", n_new - n_deduped,
          "| valid:", n_valid, "| rejected:", n_rejected)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # ECB Exchange

# CELL ********************

FX_KEYS = ["currency", "date"]

# 1. Read only the new bronze rows
new_rows = read_new_bronze_rows(BRONZE_FX, [SILVER_FX, REJECTED_FX])
n_new = new_rows.count()

if n_new == 0:
    print("FX already up to date")
    run_stats["fx"] = {"new": 0, "deduped": 0, "valid": 0, "rejected": 0}
else:
    # 2. One row per currency + date (the newest one)
    deduped = keep_latest(new_rows, FX_KEYS)
    n_deduped = deduped.count()

    # 3. One rule: the rate must exist and be positive
    reject_reason = F.when(F.col("eur_rate").isNull() | (F.col("eur_rate") <= 0), "invalid rate")
    tagged = deduped.withColumn("reject_reason", reject_reason)
    tagged = tagged.withColumn("_processed_at", F.lit(RUN_TS))

    # 4. Split valid and rejected rows
    valid = tagged.filter(F.col("reject_reason").isNull()).drop("reject_reason")
    rejected = tagged.filter(F.col("reject_reason").isNotNull())

    n_valid = valid.count()
    n_rejected = rejected.count()

    # 5. Rejected rows: appended with the reason
    if n_rejected > 0:
        rejected.write.mode("append").saveAsTable(REJECTED_FX)

    # 6. Valid rows: upsert into silver
    merge_into(SILVER_FX, valid, FX_KEYS)

    run_stats["fx"] = {"new": n_new, "deduped": n_deduped, "valid": n_valid, "rejected": n_rejected}
    print("FX -> read:", n_new, "| duplicates removed:", n_new - n_deduped,
          "| valid:", n_valid, "| rejected:", n_rejected)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Check

# CELL ********************

# 1. Row counts
for table_name in [SILVER_TICKERS, SILVER_PRICES, SILVER_FX, REJECTED_PRICES, REJECTED_FX]:
    if spark.catalog.tableExists(table_name):
        print(table_name, spark.table(table_name).count(), "rows")
    else:
        print(table_name, "does not exist yet (no rows rejected so far)")

# 2. Key must be unique: number of rows = number of distinct keys
checks = [(SILVER_PRICES, ["ticker", "date"]), (SILVER_FX, ["currency", "date"])]

for table_name, keys in checks:
    df = spark.table(table_name)
    n_rows = df.count()
    n_keys = df.select(keys).distinct().count()
    if n_rows == n_keys:
        print("OK  ", table_name, "key is unique")
    else:
        print("FAIL", table_name, "has", n_rows - n_keys, "duplicate keys")

# 3. Reconciliation of this run: deduplicated rows = valid + rejected
for name, stats in run_stats.items():
    if stats["deduped"] == stats["valid"] + stats["rejected"]:
        print("OK  ", name, "reconciled:", stats)
    else:
        print("FAIL", name, "rows lost or created:", stats)

# 4. Why rows were rejected (if any)
if spark.catalog.tableExists(REJECTED_PRICES):
    display(spark.table(REJECTED_PRICES).groupBy("reject_reason").count())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Show Results

# CELL ********************

# All rows of one table (display shows a sortable, filterable grid)
display(spark.table("silver.prices"))

# Only one ticker, newest first
display(spark.table("silver.prices")
        .filter("ticker = 'NESN.SW'")
        .orderBy(F.col("date").desc()))

# Exchange rates of the last days
display(spark.table("silver.fx_rates").orderBy(F.col("date").desc()).limit(15))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
