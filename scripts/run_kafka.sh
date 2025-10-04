orders_df.selectExpr("CAST(value AS STRING)").writeStream \
    .outputMode("append") \
    .format("console") \
    .start() \
    .awaitTermination()
