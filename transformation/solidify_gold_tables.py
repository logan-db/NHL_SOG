gold_shots = spark.table("lr_nhl_demo.dev.gold_shots")

gold_shots.write.format("delta").mode("overwrite").saveAsTable("lr_nhl_demo.dev.model_shots_game")
print("success")
