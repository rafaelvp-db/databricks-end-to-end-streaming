# Databricks notebook source
dbutils.widgets.text("source_table", "product_silver")
dbutils.widgets.text("target_table", "product_gold")
checkpoint_path = "<CHECKPOINT-PATH>"

source_table = getArgument("source_table")
target_table = getArgument("target_table")
checkpoint_location_source = f"{checkpoint_path}/{source_table}"
checkpoint_location_target = f"{checkpoint_path}/{target_table}"

# COMMAND ----------

startingOffsets = "latest"

silver_df = spark.readStream \
  .format("delta") \
  .option("startingOffsets", startingOffsets) \
  .option("checkpointLocation", checkpoint_location_source) \
  .table(source_table)

# COMMAND ----------

from pyspark.sql import functions as F
import datetime

curr_date = datetime.datetime.now().strftime("%d-%m-%Y 00:00:00")

gold_df = silver_df \
  .where(F.col("timestamp") >= curr_date) \
  .groupBy("type", "color", "size") \
  .agg(F.count("type"), F.count("color"), F.count("size"), F.last("timestamp")) \
  .withColumnRenamed("count(type)", "count_type") \
  .withColumnRenamed("count(color)", "count_color") \
  .withColumnRenamed("count(size)", "count_size") \
  .withColumnRenamed("last(timestamp)", "last")

# COMMAND ----------

gold_df.writeStream \
  .format("delta") \
  .outputMode("complete") \
  .option("checkpointLocation", checkpoint_location_target) \
  .trigger(once=True) \
  .table(target_table)