silver_skaters_enriched = spark.table("lr_nhl_demo.dev.silver_skaters_enriched")

silver_skaters_enriched.write.format("delta").mode("overwrite").saveAsTable("lr_nhl_demo.dev.silver_skaters_table")
print("success")
