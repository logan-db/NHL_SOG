def solidify_tables(
    table_name: str,
    new_table_name: str,
    catalog: str = "lr_nhl_demo",
    schema: str = "dev",
):
    table_to_solidify = spark.table(f"{catalog}.{schema}.{table_name}")

    table_to_solidify.write.format("delta").mode("overwrite").saveAsTable(
        f"{catalog}.{schema}.{new_table_name}"
    )
    print(
        f"Successfully solidified {catalog}.{schema}.{table_name} to --> {catalog}.{schema}.{new_table_name}"
    )


spark.sql("DROP TABLE IF EXISTS lr_nhl_demo.dev.gold_player_stats_delta")
spark.sql("DROP TABLE IF EXISTS lr_nhl_demo.dev.gold_game_stats_delta")
spark.sql("DROP TABLE IF EXISTS lr_nhl_demo.dev.gold_model_stats_delta")

solidify_tables("gold_player_stats", "gold_player_stats_delta")
solidify_tables("gold_game_stats", "gold_game_stats_delta")
solidify_tables("gold_model_stats", "gold_model_stats_delta")  # adding comments
