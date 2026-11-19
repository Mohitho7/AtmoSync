# DE-05 Day 3 - dbt Core and Snowflake Transformation

## Project

The dbt project is [dbt/atmosync](../dbt/atmosync). It reads
`ATMOSYNC.RAW.SENSOR_EVENTS`, creates cleaned staging models in
`ATMOSYNC.STAGING`, and creates a pricing-enriched fact model in
`ATMOSYNC.ANALYTICS`.

## Credentials

The profile at [dbt/atmosync/profiles.yml](../dbt/atmosync/profiles.yml)
uses these environment variables:

- `SNOWFLAKE_ACCOUNT`
- `SNOWFLAKE_USER`
- `SNOWFLAKE_PASSWORD`
- `SNOWFLAKE_AUTHENTICATOR`
- `SNOWFLAKE_ROLE`

The warehouse is fixed to `ATMOSYNC_WH`, the database to `ATMOSYNC`, and the
default schema to `STAGING`. Never place credentials in tracked files.

## Commands

From the repository root:

```powershell
.\.venv\Scripts\python.exe -m pip install -e ".[snowflake,dbt]"
.\.venv\Scripts\dbt.exe debug --project-dir dbt\atmosync --profiles-dir dbt\atmosync
.\.venv\Scripts\dbt.exe seed --project-dir dbt\atmosync --profiles-dir dbt\atmosync
.\.venv\Scripts\dbt.exe build --project-dir dbt\atmosync --profiles-dir dbt\atmosync
```

Use the existing `.env`; dbt does not replace Snowflake credentials with a
second credential system.
