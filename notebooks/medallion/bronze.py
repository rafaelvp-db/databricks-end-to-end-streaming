# Databricks notebook source
dbutils.widgets.text("source_table", "raw_events")
dbutils.widgets.text("target_table", "product_bronze")
checkpoint_path = "<CHECKPOINT-PATH>"

source_table = getArgument("source_table")
target_table = getArgument("target_table")
checkpoint_location_target = f"{checkpoint_path}/{target_table}"

# COMMAND ----------

startingOffsets = "latest"

bronze_df = spark.readStream \
  .format("delta") \
  .option("startingOffsets", startingOffsets) \
  .table(source_table) \
  .selectExpr("parsedValue.*")

bronze_df.writeStream \
  .format("delta") \
  .outputMode("append") \
  .option("checkpointLocation", checkpoint_location_target) \
  .option("mergeSchema", "true") \
  .trigger(once=True) \
  .partitionBy("type") \
  .table(target_table)

# COMMAND ----------

display(spark.sql(f"select count(*) from {target_table}"))

# COMMAND ----------

