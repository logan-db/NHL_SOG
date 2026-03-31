import sys

# Catalog is passed as the first positional argument by the job (e.g. "lr_nhl_demo.dev").
# Falls back to dev for interactive / ad-hoc runs.
catalog_param = sys.argv[1] if len(sys.argv) > 1 else "lr_nhl_demo.dev"
_parts = catalog_param.split(".")
catalog_name = _parts[0]
schema_name = _parts[1] if len(_parts) > 1 else "dev"


def solidify_tables(
    table_name: str,
    new_table_name: str,
    catalog: str = catalog_name,
    schema: str = schema_name,
):
    table_to_solidify = spark.table(f"{catalog}.{schema}.{table_name}")

    table_to_solidify.write.format("delta").mode("overwrite").saveAsTable(
        f"{catalog}.{schema}.{new_table_name}"
    )
    print(
        f"Successfully solidified {catalog}.{schema}.{table_name} to --> {catalog}.{schema}.{new_table_name}"
    )


spark.sql(f"DROP TABLE IF EXISTS {catalog_param}.gold_player_stats_delta_v2")
spark.sql(f"DROP TABLE IF EXISTS {catalog_param}.gold_game_stats_delta_v2")
spark.sql(f"DROP TABLE IF EXISTS {catalog_param}.gold_model_stats_delta_v2")

solidify_tables("gold_player_stats_v2", "gold_player_stats_delta_v2")
solidify_tables("gold_game_stats_v2", "gold_game_stats_delta_v2")
solidify_tables("gold_model_stats_v2", "gold_model_stats_delta_v2")

# Enable Change Data Feed on gold_game_stats_delta_v2 for synced database tables
print("Enabling Change Data Feed on gold_game_stats_delta_v2...")
spark.sql(
    f"ALTER TABLE {catalog_param}.gold_game_stats_delta_v2 SET TBLPROPERTIES (delta.enableChangeDataFeed = true)"
)
print("✅ Change Data Feed enabled on gold_game_stats_delta_v2")
