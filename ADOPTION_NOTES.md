# Adoption notes

Friction log for building fabric-market-monitor on Microsoft Fabric.
Format: date · what I tried · what blocked me · how I solved it

- 2026-11-04 · Create lakehouse with schemas · Unsure where the "Lakehouse schemas" option was · It's a checkbox in the New lakehouse dialog (on by default); schemas are added via Tables → … → New schema

- 2026-11-04 · Connect workspace to GitHub · No "Git integration" in Workspace settings · My workspace doesn't support Git integration; a regular workspace is needed

- 2026-11-04 · Create a new workspace on the Polimi account · "App workspace creation is disabled" by the tenant admin · No fix on that tenant; set up my own Azure/Entra tenant instead

- 2026-11-04 · Use Fabric with personal Outlook account · Fabric doesn't accept personal Microsoft accounts · Created a work user (simone@…onmicrosoft.com) in my own Entra tenant, made it Global Admin

- 2026-11-04 · Start Fabric trial in the new tenant · Power BI trial activated, but "A Fabric trial isn't available for your account" · Still unresolved: retried on 05/11 and 11/11 (also via "Start Fabric trial", region Italy North), same error; the tenant setting "Users can try Microsoft Fabric paid features" is already on. Decision: build in My workspace on the Polimi tenant for now. Next options: another region, a Microsoft support ticket, or a pay-as-you-go F2 capacity

- 2026-11-04 · Prepare Git connection · — · Enabled tenant settings (Fabric items, workspaces, Git/GitHub sync) and created a fine-grained PAT (Contents: read & write, this repo only)

- 2026-11-05 · Load 2019.csv with Fabric's auto-generated code (header=true) · AnalysisException UNRESOLVED_COLUMN: the file has no header row, so the first data row became column names, and names with dots (emails, prices) can't be resolved · Removed the header option (columns become _c0…_c8); next step is an explicit schema for proper names/types

- 2026-11-05 · Create a temp view in a %%sql cell (CREATE OR REPLACE TEMPORARY VIEW … FROM dbo.products_table) · "Failed to decode multipart name: 'dbo'"; the 3-part name lakehouse.dbo.table fails with REQUIRES_SINGLE_PART_NAMESPACE · Built the view in PySpark with spark.sql(...).createOrReplaceTempView(), then queried it in %%sql

- 2026-11-05 · Install yfinance for the notebooks · %pip install only lasts for the current session, and inline installs are disabled by default when a pipeline runs the notebook · Created the env_market Environment (yfinance pinned to 1.8.0, Full publish mode) and attached it to each notebook. Lesson: what works interactively isn't production-ready; treat dependencies as infrastructure

- 2026-11-05 · Use the environment right after attaching it · The notebook didn't pick up yfinance · Environment libraries load only when a session starts: stop and restart the Spark session after attaching

- 2026-11-05 · Version the work on GitHub · My workspace on the Polimi tenant can't use Git integration · Export each notebook as .ipynb and commit it by hand until a Git-enabled workspace is available

- 2026-11-12 · Create the F2 capacity from the Azure account that owns the subscription (personal Outlook) · "Unsupported account: You cannot create a Microsoft capacity using a personal account" · Gave the tenant user (simone@…onmicrosoft.com) the Contributor role on the subscription (IAM), then created the capacity as that user. Lesson: billing owner and Fabric user are two different identities

- 2026-11-12 · Keep building on the Polimi tenant · Banner: "Your free Microsoft Fabric trial ends in 2 days"; after expiry, non-Power BI items (lakehouse, notebooks, environment) become unusable and are deleted after a 7-day grace period · Open: exported all notebooks to the repo; [what you decide]

- 2026-11-12 · Connect and sync the workspace to GitHub · "Credentials for the Git provider are not authorized" (branches were listed, so read worked) · The fine-grained PAT had Contents: Read only; changed it to Read and write and the sync succeeded

- 2026-11-12 · First bronze load of 15 SMI/FTSE MIB stocks via yfinance · ROG.SW returned 404 "Quote not found" (the line seems inactive since March 2026); LONN.SW hit a network timeout · Switched Roche to RO.SW in tickers.csv; made the watermark per ticker and grouped downloads by start date so a new ticker is backfilled without re-downloading the others; missing tickers are reported and retried on the next run

- 2026-11-12 · Append RO.SW prices to bronze.prices_raw · DELTA_FAILED_TO_MERGE_FIELDS on 'volume': first load was int64 (bigint), RO.SW had missing volumes so pandas made it float64 (double) · Added a fixed bronze schema contract: NaN → null and explicit casts before every write. Lesson: never let pandas type inference define a Delta table's schema

- 2026-11-14 · Moved data quality checks into a dedicated notebook · Rejected tables don’t exist until the first reject, so a naive check would error · Treated a missing rejected table as “passed, 0 rows” and documented it.

- 2026-11-12 · Append RO.SW prices to bronze.prices_raw · DELTA_FAILED_TO_MERGE_FIELDS on 'volume': yf.download aligns several tickers on one calendar, so different holidays (CH vs IT) create NaNs and pandas turns volume into float64; the first load had created the column as double · Dropped the bronze table and reloaded it with a fixed schema contract (NaN → null, explicit casts before every write). Lesson: never let pandas type inference define a Delta table's schema

- 2026-11-15 · First pipeline run on a Sunday · ECB API returned HTTP 200 with an empty body (not 404) for a weekend-only range → pandas `EmptyDataError` in bronze; the retries ran but couldn't help because the error was deterministic · Treated an empty body as "no new data", same as 404.

- 2026-11-15 · Second pipeline run · Bronze stayed queued for 10+ minutes: on an F2 capacity, the interactive sessions of five open notebooks were using the Spark cores the pipeline needed · Stopped the interactive sessions; rule: close them before running the pipeline.

- 2026-11-15 · Linking `pipeline_runs` and `dq_results` on `run_id` · The pipeline was all green, but the join returned 0 checks for the pipeline run: the `run_id` base parameter was missing on the `nb_04_dq` activity, so the notebook generated its own id · Added `run_id = @pipeline().RunId`; lesson: verify the data links, not only the green status.

- 2026-11-16 · Testing the failure branch by setting a 1-minute timeout on silver · Fabric rejects activity timeouts below 10 minutes · Added a temporary `raise` cell to silver, ran the pipeline, then removed the cell before committing.

- 2026-11-16 · Querying gold tables from the SQL analytics endpoint · Metadata sync failed ("There is already an object named 'gold'", CREATE SCHEMA failed) on the schema-enabled lakehouse; new rows also showed up with a short delay · Retry sync / page reload; Spark notebooks read the Delta tables directly as a fallback.

- 2026-11-16 · Reducing run time by dropping `env_market` and installing yfinance with `%pip` in bronze · `%pip install` is not supported in high concurrency mode and is disabled by default in pipeline runs · Did the opposite: `env_market` on all five notebooks + high concurrency for pipelines + one session tag → 546 s → 269 s.
