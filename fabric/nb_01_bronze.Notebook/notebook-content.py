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

from datetime import date, datetime, timedelta, timezone
import io

import pandas as pd
import requests
import yfinance as yf
from pyspark.sql import functions as F

# First date to download when a table is still empty
START_DEFAULT = date(2019, 1, 1)

# Path of the tickers file, relative to the default lakehouse (lh_market)
TICKERS_PATH = "Files/reference/tickers.csv"

# Bronze tables
TICKERS_TABLE = "bronze.tickers_raw"
PRICES_TABLE = "bronze.prices_raw"
FX_TABLE = "bronze.fx_raw"

# Time of this run in UTC.
# We remove the time zone info because Spark timestamps don't store it
# (a value with a time zone could be shifted during the conversion)
now_utc = datetime.now(timezone.utc)
RUN_TS = now_utc.replace(tzinfo=None)

# Day of this run
LOAD_DATE = RUN_TS.date()

# END is excluded: only load completed days, never today's partial prices
END = LOAD_DATE

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Support  functions

# CELL ********************

# Lineage columns: added to every bronze table to know where each row comes from
LINEAGE_SCHEMA = {
    "_ingested_at": "timestamp",   # when the row arrived
    "_source": "string",           # where it comes from: yfinance or EBC (BCE)
    "load_date": "date",           # day of the load
}


def get_last_date(table_name):
    """Watermark of a whole table: the most recent date, or None if the table doesn't exist yet."""
    if not spark.catalog.tableExists(table_name):
        return None

    df = spark.table(table_name)
    result = df.agg(F.max("date").alias("last_date")).first()
    return result["last_date"]


def get_last_date_per_ticker(table_name):
    """Watermark per ticker: {ticker: most recent date}. Empty dictionary if the table doesn't exist yet."""
    last_dates = {}

    if not spark.catalog.tableExists(table_name):
        return last_dates

    df = spark.table(table_name)
    rows = df.groupBy("ticker").agg(F.max("date").alias("last_date")).collect()

    for row in rows:
        last_dates[row["ticker"]] = row["last_date"]

    return last_dates


def append_to_bronze(pandas_df, schema, table_name, source_name):
    """Add lineage columns, apply the schema contract and append the rows to a bronze table."""

    # Work on a copy, so we don't change the DataFrame of the caller
    pandas_df = pandas_df.copy()

    # Add the lineage columns
    pandas_df["_ingested_at"] = RUN_TS
    pandas_df["_source"] = source_name
    pandas_df["load_date"] = LOAD_DATE

    # Full contract = data columns + lineage columns
    full_schema = dict(schema)
    full_schema.update(LINEAGE_SCHEMA)

    # From pandas to Spark
    spark_df = spark.createDataFrame(pandas_df)

    # Prepare every column with the type written in the contract
    final_columns = []
    for column_name, column_type in full_schema.items():
        column = F.col(column_name)

        # Numeric columns: NaN ("not a number") becomes null ("missing value")
        if column_type == "double" or column_type == "bigint":
            column = column.cast("double")
            column = F.nanvl(column, F.lit(None).cast("double"))

        # Cast to the contract type and keep the original name
        column = column.cast(column_type).alias(column_name)
        final_columns.append(column)

    # Keep only the contract columns, in the contract order
    spark_df = spark_df.select(final_columns)

    # One single append = one Delta transaction (all rows or nothing)
    spark_df.write.mode("append").saveAsTable(table_name)

    return len(pandas_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Ticker Table

# CELL ********************

# Read the CSV
tickers_df = spark.read.option("header", True).csv(TICKERS_PATH)

# Add lineage columns
tickers_df = tickers_df.withColumn("_ingested_at", F.lit(RUN_TS))
tickers_df = tickers_df.withColumn("_source", F.lit("tickers.csv"))

# Reference table: we only need today's version, so we overwrite it
tickers_df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(TICKERS_TABLE)

# List of symbols to download (update the CSV symbol column, not the code)
symbols = []
for row in tickers_df.select("symbol").collect():
    symbols.append(row["symbol"])

print("Tickers:", len(symbols))
print(symbols)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Prices

# CELL ********************

# Schema contract of the prices table
PRICE_SCHEMA = {
    "date": "date",
    "ticker": "string",
    "open": "double",
    "high": "double",
    "low": "double",
    "close": "double",
    "adj_close": "double",
    "volume": "bigint",
}

# Last loaded date for each ticker (empty on the first run)
last_dates = get_last_date_per_ticker(PRICES_TABLE)

downloaded = []   # DataFrames downloaded in this run
missing = []      # tickers that returned no data

for ticker in symbols:

    # 1. Where do we start for this ticker?
    if ticker in last_dates:
        start_date = last_dates[ticker] + timedelta(days=1)
    else:
        start_date = START_DEFAULT

    # 2. Already up to date? Go to the next ticker
    if start_date >= END:
        continue

    # 3. Download the daily prices (END is excluded)
    df = yf.download(
        ticker,
        start=start_date.isoformat(),
        end=END.isoformat(),
        auto_adjust=False,         # keep both close and adj_close
        multi_level_index=False,   # simple column names
        progress=False,            # no progress bar in the logs
    )

    # 4. No data? Remember the ticker and continue with the others
    if df.empty:
        missing.append(ticker)
        continue

    # 5. Clean the shape of the data
    df = df.reset_index()   # the date becomes a normal column

    new_column_names = []
    for column_name in df.columns:
        new_column_names.append(column_name.lower().replace(" ", "_"))   # "Adj Close" -> "adj_close"
    df.columns = new_column_names

    df["date"] = pd.to_datetime(df["date"]).dt.date   # keep only the date, no time
    df["ticker"] = ticker

    downloaded.append(df)

# 6. Write all tickers together, in one single append
if len(downloaded) > 0:
    all_prices = pd.concat(downloaded, ignore_index=True)
    rows_written = append_to_bronze(all_prices, PRICE_SCHEMA, PRICES_TABLE, "yfinance")
    print(rows_written, "price rows appended")
else:
    print("Prices already up to date")

if len(missing) > 0:
    print("WARNING - no data for:", missing, "-> they will be retried on the next run")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # BCE Exchange

# CELL ********************

# Schema contract of the FX table
FX_SCHEMA = {
    "date": "date",
    "currency": "string",
    "eur_rate": "double",
}

# Where do we start? (one watermark: all currencies arrive together)
last_fx_date = get_last_date(FX_TABLE)

if last_fx_date is None:
    fx_start = START_DEFAULT
else:
    fx_start = last_fx_date + timedelta(days=1)

# The ECB end date is INCLUDED, so we stop at yesterday (same rule as prices)
fx_end = END - timedelta(days=1)

# Anything to download?
if fx_start > fx_end:
    print("FX already up to date")
else:
    base_url = "https://data-api.ecb.europa.eu/service/data/EXR/D.CHF+USD+GBP.EUR.SP00.A"
    url = base_url + "?format=csvdata&startPeriod=" + str(fx_start) + "&endPeriod=" + str(fx_end)

    # Never wait forever: stop after 60 seconds
    response = requests.get(url, timeout=60)

    if response.status_code == 404:
        # ECB convention: 404 = no data in this period (weekend, holidays)
        print("No new FX observations")
    else:
        # Any other HTTP error stops the notebook (fail loudly)
        response.raise_for_status()

        fx_df = pd.read_csv(io.StringIO(response.text))
        fx_df = fx_df[["TIME_PERIOD", "CURRENCY", "OBS_VALUE"]]
        fx_df = fx_df.rename(columns={
            "TIME_PERIOD": "date",
            "CURRENCY": "currency",
            "OBS_VALUE": "eur_rate",
        })
        fx_df["date"] = pd.to_datetime(fx_df["date"]).dt.date

        rows_written = append_to_bronze(fx_df, FX_SCHEMA, FX_TABLE, "ecb")
        print(rows_written, "FX rows appended")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Check

# CELL ********************

# 1. Row count of each table
for table_name in [TICKERS_TABLE, PRICES_TABLE, FX_TABLE]:
    row_count = spark.table(table_name).count()
    print(table_name, row_count, "rows")

# 2. Schema of the prices table (volume must be long = bigint)
spark.table(PRICES_TABLE).printSchema()

# 3. Coverage per ticker
prices = spark.table(PRICES_TABLE)
prices_coverage = prices.groupBy("ticker").agg(
    F.min("date").alias("first_date"),
    F.max("date").alias("last_date"),
    F.count("*").alias("rows"),
    F.sum(F.when(F.col("volume").isNull(), 1).otherwise(0)).alias("null_volumes"),
)
display(prices_coverage.orderBy("ticker"))

# 4. Coverage per currency
fx = spark.table(FX_TABLE)
fx_coverage = fx.groupBy("currency").agg(
    F.min("date").alias("first_date"),
    F.max("date").alias("last_date"),
    F.count("*").alias("rows"),
)
display(fx_coverage.orderBy("currency"))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
