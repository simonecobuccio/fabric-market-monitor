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

# # Parameters and Writing Functions

# CELL ********************

from pyspark.sql import functions as F

# Annualize daily volatility: about 252 trading days in a year
TRADING_DAYS_PER_YEAR = 252


def replace_table(query, table_name):
    """Run a SQL query and rebuild the gold table with its result (full rebuild = idempotent)."""
    df = spark.sql(query)
    df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(table_name)
    print(table_name, spark.table(table_name).count(), "rows")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Dim Ticker

# CELL ********************

replace_table("""
    SELECT
        symbol    AS ticker,      -- natural key of the dimension
        name,
        sector,
        country,
        currency,
        exchange
    FROM silver.tickers
""", "gold.dim_ticker")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Dim Date

# CELL ********************

replace_table("""
    WITH bounds AS (
        -- first and last day we have prices for
        SELECT MIN(date) AS first_day, MAX(date) AS last_day
        FROM silver.prices
    ),
    days AS (
        -- one row for EVERY calendar day between the two dates
        SELECT EXPLODE(SEQUENCE(first_day, last_day, INTERVAL 1 DAY)) AS date
        FROM bounds
    )
    SELECT
        CAST(DATE_FORMAT(date, 'yyyyMMdd') AS INT) AS date_key,    -- e.g. 20261113
        date,
        YEAR(date)                 AS year,
        QUARTER(date)              AS quarter,
        MONTH(date)                AS month,
        DATE_FORMAT(date, 'MMMM')  AS month_name,
        DAYOFWEEK(date)            AS day_of_week,                 -- 1 = Sunday ... 7 = Saturday
        DATE_FORMAT(date, 'EEEE')  AS day_name,
        DAYOFWEEK(date) IN (1, 7)  AS is_weekend,
        date = LAST_DAY(date)      AS is_month_end
    FROM days
""", "gold.dim_date")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # CHF Exchange - Temporary View

# CELL ********************

fx_to_chf = spark.sql("""
    WITH all_days AS (
        -- trading days + ECB publication days (UNION removes duplicates)
        SELECT date FROM silver.prices
        UNION
        SELECT date FROM silver.fx_rates
    ),
    currencies AS (
        SELECT DISTINCT currency FROM silver.fx_rates          -- CHF, GBP, USD
    ),
    grid AS (
        -- every day x every currency
        SELECT d.date, c.currency
        FROM all_days d
        CROSS JOIN currencies c
    ),
    joined AS (
        -- the published rate, or null when the ECB did not publish that day
        SELECT g.date, g.currency, r.eur_rate
        FROM grid g
        LEFT JOIN silver.fx_rates r
            ON g.date = r.date AND g.currency = r.currency
    ),
    filled AS (
        -- forward fill: if the rate is null, use the last known rate of that currency
        SELECT
            date,
            currency,
            LAST(eur_rate, true) OVER (
                PARTITION BY currency
                ORDER BY date
                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
            ) AS eur_rate
        FROM joined
    ),
    with_eur AS (
        -- add the euro itself: 1 EUR = 1 EUR
        SELECT date, currency, eur_rate FROM filled
        UNION ALL
        SELECT date, 'EUR' AS currency, 1.0 AS eur_rate FROM all_days
    ),
    chf AS (
        -- how many CHF for 1 EUR, day by day
        SELECT date, eur_rate AS chf_per_eur
        FROM filled
        WHERE currency = 'CHF'
    )
    -- cross rate: CHF per 1 unit of X = (CHF per EUR) / (X per EUR)
    SELECT
        w.date,
        w.currency,
        c.chf_per_eur / w.eur_rate AS rate_to_chf
    FROM with_eur w
    JOIN chf c ON w.date = c.date
""")

fx_to_chf.createOrReplaceTempView("fx_to_chf")

# Quick look: on the same day CHF must be 1.0 and EUR must be the CHF per EUR rate
display(spark.sql("SELECT * FROM fx_to_chf WHERE date = (SELECT MAX(date) FROM fx_to_chf) ORDER BY currency"))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Fact Daily Price

# CELL ********************

replace_table(f"""
    WITH base AS (
        -- STEP A: prices + currency of the stock + exchange rate of that day
        SELECT
            CAST(DATE_FORMAT(p.date, 'yyyyMMdd') AS INT) AS date_key,
            p.date,
            p.ticker,
            t.currency,
            p.open, p.high, p.low, p.close, p.adj_close, p.volume,
            fx.rate_to_chf,
            p.close     * fx.rate_to_chf AS close_chf,
            p.adj_close * fx.rate_to_chf AS adj_close_chf
        FROM silver.prices p
        JOIN gold.dim_ticker t
            ON p.ticker = t.ticker
        LEFT JOIN fx_to_chf fx                                  -- LEFT: a missing rate stays visible as null
            ON p.date = fx.date AND t.currency = fx.currency
    ),
    returns AS (
        -- STEP B: daily return = today / yesterday - 1 (first day of each ticker = null)
        SELECT
            *,
            adj_close     / LAG(adj_close)     OVER (PARTITION BY ticker ORDER BY date) - 1 AS daily_return,
            adj_close_chf / LAG(adj_close_chf) OVER (PARTITION BY ticker ORDER BY date) - 1 AS daily_return_chf
        FROM base
    ),
    metrics AS (
        -- STEP C: rolling windows per ticker
        SELECT
            *,
            COUNT(*)             OVER w20 AS rows_in_20,
            AVG(close)           OVER w20 AS avg_close_20,
            COUNT(daily_return)  OVER w30 AS returns_in_30,
            STDDEV(daily_return) OVER w30 AS std_return_30,
            MAX(adj_close) OVER (
                PARTITION BY ticker ORDER BY date
                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
            ) AS running_max
        FROM returns
        WINDOW
            w20 AS (PARTITION BY ticker ORDER BY date ROWS BETWEEN 19 PRECEDING AND CURRENT ROW),
            w30 AS (PARTITION BY ticker ORDER BY date ROWS BETWEEN 29 PRECEDING AND CURRENT ROW)
    )
    SELECT
        date_key, date, ticker, currency,
        open, high, low, close, adj_close, volume,
        rate_to_chf, close_chf, adj_close_chf,
        daily_return, daily_return_chf,
        -- not enough rows in the window -> null (a "20-day average" of 5 days would be wrong)
        CASE WHEN rows_in_20 = 20    THEN avg_close_20 END                                    AS ma_20,
        CASE WHEN returns_in_30 = 30 THEN std_return_30 * SQRT({TRADING_DAYS_PER_YEAR}) END    AS vol_30d,
        adj_close / running_max - 1                                                           AS drawdown
    FROM metrics
""", "gold.fact_daily_price")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Latest Snapshot

# CELL ********************

replace_table("""
    WITH ranked AS (
        -- number the rows of each ticker from the newest day
        SELECT *, ROW_NUMBER() OVER (PARTITION BY ticker ORDER BY date DESC) AS rn
        FROM gold.fact_daily_price
    ),
    last_row AS (
        SELECT * FROM ranked WHERE rn = 1
    ),
    prev_year_end AS (
        -- last trading day of the PREVIOUS year = base of the year-to-date return
        SELECT
            f.ticker,
            f.adj_close     AS base_adj_close,
            f.adj_close_chf AS base_adj_close_chf,
            ROW_NUMBER() OVER (PARTITION BY f.ticker ORDER BY f.date DESC) AS rn
        FROM gold.fact_daily_price f
        JOIN last_row l ON f.ticker = l.ticker
        WHERE YEAR(f.date) = YEAR(l.date) - 1
    )
    SELECT
        l.ticker,
        t.name,
        t.sector,
        t.currency,
        l.date                                         AS last_date,
        l.close,
        l.close_chf,
        l.adj_close     / y.base_adj_close     - 1     AS ytd_return,
        l.adj_close_chf / y.base_adj_close_chf - 1     AS ytd_return_chf,
        l.vol_30d,
        l.drawdown
    FROM last_row l
    JOIN prev_year_end y   ON l.ticker = y.ticker AND y.rn = 1
    JOIN gold.dim_ticker t ON l.ticker = t.ticker
""", "gold.latest_snapshot")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# # Checks

# CELL ********************

def report(check_name, passed, detail):
    if passed:
        print("OK  ", check_name, "-", detail)
    else:
        print("FAIL", check_name, "-", detail)


fact = spark.table("gold.fact_daily_price")

# 1. Grain: one row per ticker per day
n_fact = fact.count()
n_keys = fact.select("ticker", "date").distinct().count()
report("grain", n_fact == n_keys, str(n_fact) + " rows, " + str(n_keys) + " distinct keys")

# 2. Completeness: no row lost in the joins
n_silver = spark.table("silver.prices").count()
report("completeness", n_fact == n_silver, str(n_fact) + " fact rows vs " + str(n_silver) + " silver rows")

# 3. Every row has an exchange rate
n_null_fx = fact.filter(F.col("rate_to_chf").isNull()).count()
report("fx coverage", n_null_fx == 0, str(n_null_fx) + " rows without rate_to_chf")

# 4. Referential integrity: every key of the fact exists in its dimension (anti join = rows WITHOUT a match)
n_missing_dates = spark.sql("""
    SELECT COUNT(*) AS n
    FROM gold.fact_daily_price f
    LEFT ANTI JOIN gold.dim_date d ON f.date_key = d.date_key
""").first()["n"]

n_missing_tickers = spark.sql("""
    SELECT COUNT(*) AS n
    FROM gold.fact_daily_price f
    LEFT ANTI JOIN gold.dim_ticker t ON f.ticker = t.ticker
""").first()["n"]

report("keys", n_missing_dates == 0 and n_missing_tickers == 0,
       str(n_missing_dates) + " missing dates, " + str(n_missing_tickers) + " missing tickers")

# 5. The result
display(spark.table("gold.latest_snapshot").orderBy("ticker"))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
