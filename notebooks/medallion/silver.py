# Databricks notebook source
dbutils.widgets.text("source_table", "product_bronze")
dbutils.widgets.text("target_table", "product_silver")
checkpoint_path = "<CHECKPOINT-PATH>"

source_table = getArgument("source_table")
target_table = getArgument("target_table")
checkpoint_location_target = f"{checkpoint_path}/{target_table}"

# COMMAND ----------

from pyspark.sql import functions as F

startingOffsets = "latest"

silver_df = spark.readStream \
  .format("delta") \
  .option("startingOffsets", startingOffsets) \
  .table(source_table)

#Remove duplicates & parse timestamp into date time format

silver_df = silver_df.dropDuplicates(["eventId"])
silver_df = silver_df.withColumn(
  "timestamp",
  F.from_unixtime("timestamp", "dd-MM-yyyy H:mm:ss")
)

#Write Stream

silver_df.writeStream \
  .format("delta") \
  .outputMode("append") \
  .option("mergeSchema", "true") \
  .option("checkpointLocation", checkpoint_location_target) \
  .trigger(once=True) \
  .partitionBy("type") \
  .table(target_table)

# COMMAND ----------

