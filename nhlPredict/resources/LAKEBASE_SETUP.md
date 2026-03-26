# Lakebase Catalog Setup for NHL App

The NHL app requires catalog `lr-lakebase` with synced tables from `lr_nhl_demo.dev`.

## Option 1: Autoscale (lr-database-instance)

You have autoscale project `lr-database-instance`. The bundle cannot create a UC catalog from autoscale; register manually:

1. **Catalog Explorer** → **Create** → **Lakebase Postgres**
2. Select **Autoscaling**
3. Choose project **lr-database-instance**, branch (e.g. production), database **databricks_postgres**
4. Catalog name: **lr-lakebase**
5. Create

Then deploy: `databricks bundle deploy --target dev` (NHLLakebaseSync is included; catalog exists).

## Option 2: Provisioned (bundle-created)

If you have provisioned instance quota, add to `databricks.yml` include (before NHLLakebaseSync):

```yaml
- resources/NHLLakebaseCatalog.yml
```

This creates instance `lr-lakebase` (CU_1) and registers it as catalog `lr-lakebase`.

## Option 3: At quota

If deploy fails with "workspace limit", comment out both Lakebase lines in `databricks.yml` include. Deploy the rest, then add Lakebase back when quota is available.
