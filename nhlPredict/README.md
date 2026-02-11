# NHL Shot on Goal Prediction Pipeline

Machine learning pipeline for predicting NHL shots on goal using Databricks Delta Live Tables (DLT).

## Overview

This project implements a complete data pipeline for NHL analytics:
- **Bronze Layer:** Raw NHL game data ingested from NHL API
- **Silver Layer:** Cleaned and transformed player/team statistics
- **Gold Layer:** Aggregated ML-ready features
- **ML Models:** Shot prediction models trained on historical data

### Data Source

Uses the official NHL API via [`nhl-api-py`](https://pypi.org/project/nhl-api-py/) to ingest:
- Play-by-play game events
- Player shift data
- Boxscore statistics
- Team performance metrics

## Architecture

```
NHL API → Bronze (DLT) → Silver (DLT) → Gold (DLT) → ML Models
```

**Pipeline:**
- `01-bronze-ingestion-nhl-api.py` - Ingests NHL API data with MoneyPuck-compatible schema
- `02-silver-transform.py` - Transforms and enriches player statistics
- `03-gold-agg.py` - Aggregates features for ML models

**Incremental Loading:**
- Daily updates with 3-day lookback for corrections
- Primary key deduplication prevents duplicates
- Automatic downstream refreshes (DLT managed)

## Quick Start

### Prerequisites
```bash
# Install Databricks CLI
pip install databricks-cli

# Configure authentication
databricks configure
```

### Deploy Pipeline
```bash
# Deploy to dev environment
databricks bundle deploy --profile e2-demo-field-eng

# Run pipeline via Databricks UI
# Workflows → Pipelines → NHLPlayerIngestion → Start
```

### Run Tests
```bash
# Schema validation (fast, no PySpark required)
./run_schema_tests.sh

# Full test suite including API integration
./run_schema_tests.sh --full
```

## Configuration

### Initial Historical Load
```yaml
# resources/NHLPlayerIngestion.yml
one_time_load: "true"   # Load full historical data
lookback_days: "3"
```

Loads data from October 1, 2023 to present (~6,500 games, 3-5 hour runtime).

### Daily Incremental Mode
```yaml
# resources/NHLPlayerIngestion.yml
one_time_load: "false"  # Daily incremental updates
lookback_days: "3"      # Process last 3 days
```

Processes last 3 days (~35 games, 10-20 minute runtime).

## Project Structure

```
nhlPredict/
├── cursor_docs/              # Migration documentation
├── src/
│   ├── dlt_etl/
│   │   ├── ingestion/       # Bronze layer (NHL API ingestion)
│   │   ├── transformation/  # Silver layer (feature engineering)
│   │   └── aggregation/     # Gold layer (ML-ready features)
│   ├── utils/
│   │   ├── nhl_api_helper.py       # NHL API parsing logic
│   │   └── ingestionHelper.py      # DLT helper functions
│   ├── features/            # Feature engineering notebooks
│   ├── ML/                  # Model training and inference
│   └── BI/                  # Dashboards and analytics
├── tests/                   # Test suites
│   ├── test_schema_simple.py           # Schema validation
│   └── test_nhl_api_integration.py     # API integration tests
├── resources/               # Databricks asset bundle configs
│   └── NHLPlayerIngestion.yml          # DLT pipeline config
└── README.md               # This file
```

## Key Features

### Schema Preservation
- Maintains MoneyPuck-compatible schema at bronze layer
- Zero changes required to silver/gold layers
- ML models work without retraining

### Situation Analysis
- Automatically derives game situations from play-by-play data
- Supports: `all`, `5on5`, `5on4` (PP), `4on5` (PK)
- Enables power play, penalty kill, and even strength analytics

### Performance Optimized
- Serverless compute with auto-scaling
- Photon acceleration enabled
- Primary key deduplication for efficiency
- 3-day lookback minimizes reprocessing

### Data Quality
- Comprehensive schema validation tests
- NHL API integration tests
- DLT expectations enforce data quality rules
- Change data feed for audit trail

## Development

### Local Testing
```bash
# Run schema validation (no Databricks required)
cd tests
pytest test_schema_simple.py -v

# Run API integration tests (requires NHL API access)
pytest test_nhl_api_integration.py -v
```

### Adding New Features
1. Modify bronze schema in `01-bronze-ingestion-nhl-api.py`
2. Update parser logic in `src/utils/nhl_api_helper.py`
3. Add tests in `tests/test_schema_simple.py`
4. Deploy and validate

## Documentation

See `cursor_docs/` for comprehensive migration documentation:
- **FULL_HISTORICAL_LOAD_READY.md** - Initial load guide
- **INCREMENTAL_QUICK_START.md** - Daily incremental setup
- **MIGRATION_STATUS.md** - Current project status
- **cursor_docs/README.md** - Complete documentation index

## Monitoring

### Key Metrics
```sql
-- Check latest data
SELECT MAX(gameDate) as latest_game, COUNT(*) as total_records
FROM bronze_player_game_stats_v2;

-- Daily volume
SELECT gameDate, COUNT(DISTINCT gameId) as games
FROM bronze_player_game_stats_v2
WHERE situation = 'all'
GROUP BY gameDate
ORDER BY gameDate DESC
LIMIT 7;
```

### Expected Daily Volume
- **Games:** 8-15 per day (regular season)
- **Player records:** 600-1,200 per day
- **Runtime:** 10-20 minutes (incremental)

## Troubleshooting

See `cursor_docs/KNOWN_ISSUES.md` for common issues and fixes.

**Common issues:**
- Empty tables → Check season format (8-digit: `20232024`)
- Blank names → Verify player name extraction from roster
- Schema errors → Run `./run_schema_tests.sh` to validate

## Technology Stack

- **Platform:** Databricks (Unity Catalog, DLT)
- **Language:** Python 3.x, PySpark
- **Data Source:** NHL API via `nhl-api-py`
- **Testing:** pytest
- **Deployment:** Databricks Asset Bundles

## License

Internal project for NHL analytics and predictions.

## Contact

For questions or issues, see documentation in `cursor_docs/` or run test suite for validation.
