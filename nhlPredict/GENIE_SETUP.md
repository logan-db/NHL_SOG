# NHL SOG Genie Setup

This guide covers creating and configuring the NHL SOG Analytics Genie Space for the app.

## Overview

The **Ask** tab in the NHL Predict app uses a [Genie Space](https://docs.databricks.com/aws/en/genie/) to answer natural language questions about:

- Player analysis and stats
- Historical games and scores
- Predicted shots on goal (SOG)

## Option A: Asset Bundle (when supported)

If your Databricks CLI supports `genie_spaces` (see [PR #4191](https://github.com/databricks/cli/pull/4191)):

```bash
# Requires direct deploy mode
DATABRICKS_BUNDLE_ENGINE=direct databricks bundle deploy -t dev
```

The bundle creates the Genie Space from `resources/NHLAnalyticsGenie.yml`. You may need to set `genie_warehouse_id` in `databricks.yml` variables to match your SQL warehouse.

## Option B: Manual Creation

If bundle deploy fails or genie_spaces isn't supported:

1. **Create the Genie Space**
   - Go to **Databricks > Dashboards > Genie > Create space**
   - Name: **NHL SOG Analytics**
   - Add tables: `lr_nhl_demo.dev.clean_prediction_summary`, `lr_nhl_demo.dev.gold_player_stats_clean`, `lr_nhl_demo.dev.gold_game_stats_clean`
   - Select a SQL warehouse (e.g. `dbdemos-shared-endpoint`)
   - Add sample questions: "Who has the highest predicted SOG?", "Show me top shooters from last 10 games", etc.

2. **Add as App Resource**
   - Open your app in **Databricks > Apps > nhl-predict-app**
   - Go to **Configure > Resources**
   - Click **+ Add resource > Genie space**
   - Select the NHL SOG Analytics space
   - Permission: **Can run**
   - Resource key: `genie-space` (default)

3. **Redeploy the app** so it receives `GENIE_SPACE_ID` via the genie-space resource.

## Agent State & Lakebase

**Genie Spaces** provide built-in *short-term* conversation context: each session has a `conversation_id` that enables follow-up questions ("break that down by team", "show top 5 only") within the same chat.

For **long-term memory** (across sessions) and Lakebase-backed agent state, use the [Stateful Agents](https://docs.databricks.com/aws/en/generative-ai/agent-framework/stateful-agents) framework:

- Clone a template: `agent-langgraph-short-term-memory` or `agent-langgraph-long-term-memory`
- Configure Lakebase for checkpointing (conversation state) or long-term memory extraction
- Use the Genie Space as a tool inside a LangGraph/OpenAI agent if you want both: custom agent orchestration + Genie for data questions

The current app uses Genie directly for simplicity; conversation context is session-scoped via Genie's `conversation_id`.

## Permissions

Ensure the app's service principal has:

- **CAN RUN** on the Genie Space
- **USE CATALOG**, **USE SCHEMA**, **SELECT** on `lr_nhl_demo.dev` tables

## Troubleshooting

| Symptom | Fix |
|---------|-----|
| "GENIE_SPACE_ID not configured" | Add genie-space resource in Apps UI (Configure > Resources) |
| Empty or generic responses | Verify tables are populated; Genie queries Unity Catalog |
| "Permission denied" | Grant CAN_RUN on Genie space; SELECT on UC tables |
