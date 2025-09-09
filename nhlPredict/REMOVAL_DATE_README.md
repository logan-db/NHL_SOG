# Dynamic Removal Date for Lakebase Resources

This bundle includes a dynamic removal date system that automatically sets the removal date to 2 months from the current date.

## How It Works

- **Variable**: `removal_date` in `databricks.yml`
- **Default Value**: Automatically calculated as today + 2 months
- **Usage**: Referenced in comments as `${var.removal_date}`

## Scripts Available

### 1. `update_removal_date.sh`
Updates only the removal date in the configuration:
```bash
./update_removal_date.sh
```

### 2. `deploy_with_removal_date.sh` (Recommended)
Updates the removal date AND deploys the bundle:
```bash
./deploy_with_removal_date.sh
```

## Current Configuration

The removal date is set in the following files:
- **`databricks.yml`**: Variable definition with current date
- **`resources/LakebaseCatalog.yml`**: Comment referencing the variable

## Example Output

When you run the deployment script, you'll see:
```
🚀 Deploying NHL SOG Bundle with Dynamic Removal Date
==================================================
📅 Setting removal date to: 2025-11-09
📝 Updating databricks.yml with new removal date...
✅ Validating bundle configuration...
🚀 Deploying bundle...
✅ Deployment complete!
📅 Resources marked for removal on: 2025-11-09
```

## Manual Override

If you need to set a specific removal date, you can:
1. Edit `databricks.yml` and change the `default` value in the `removal_date` variable
2. Or use the CLI: `databricks bundle deploy --var="removal_date=2025-12-25"`

## Change Data Feed Configuration

Change Data Feed is automatically enabled when the source tables are created/updated:
- **Source Table 1**: `lr_nhl_demo.dev.clean_prediction_summary` (enabled in `/src/BI/05-Prep-Predicton-Data.py`)
- **Source Table 2**: `lr_nhl_demo.dev.gold_game_stats_clean` (enabled in `/src/genie/Prep Genie Data.py`)

**Why Change Data Feed?**
- Enables incremental updates from Delta tables
- Required for `TRIGGERED` scheduling policy
- Allows synced tables to update when source tables change
- **Automatically configured** in the data pipelines

## Resources Marked for Removal

The following resources are marked with the dynamic removal date:
- `lr-lakebase-catalog.public.nhl_player_data`
- `lr-lakebase-catalog.public.nhl_team_data`
