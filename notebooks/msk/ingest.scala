// Databricks notebook source
// MAGIC %md
// MAGIC ## Ingesting Into Raw Layer

// COMMAND ----------

import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider
import software.amazon.awssdk.auth.credentials.InstanceProfileCredentialsProvider
import com.amazonaws.services.schemaregistry.utils.AWSSchemaRegistryConstants
import com.amazonaws.services.schemaregistry.common.Schema
import com.amazonaws.services.schemaregistry.common.configs.GlueSchemaRegistryConfiguration
import com.amazonaws.services.schemaregistry.deserializers.GlueSchemaRegistryDeserializationFacade
import com.amazonaws.services.schemaregistry.common.configs._
import java.util.Properties
import java.nio.ByteBuffer
import java.util.UUID
import org.apache.spark.sql.functions._
import org.apache.spark.sql.avro.functions._

// COMMAND ----------

// get secret credentials
val kafka_bootstrap_servers_plaintext = dbutils.secrets.get( "<SECRET-SCOPE>", "<KAFKA-BOOTSTRAP-SERVERS-SECRET>" )
val topic = "<TOPIC-NAME>"
val checkpointPath = s"<CHECKPOINT-PATH>"
val kafkaCheckPointPath = s"${checkpointPath}/kafka"
val tableCheckPointPath = s"${checkpointPath}/raw_table"

// COMMAND ----------

// MAGIC %md
// MAGIC ## Magic bytes
// MAGIC AWS Glue Schema Registry embeds schema information at the beginning of the message using the following rule:
// MAGIC <br/>
// MAGIC <br/>
// MAGIC * HEADER VERSION (1 byte) | COMPRESSION (1 byte) | SCHEMA VERSION ID (16 bytes)
// MAGIC * Schema version ID is expressed as an UUID (8 high bytes + 8 low bytes)

// COMMAND ----------

//The sizes and positions of these are defined in the Glue documentation/definition

val headerSize = (AWSSchemaRegistryConstants.HEADER_VERSION_BYTE_SIZE 
                  + AWSSchemaRegistryConstants.COMPRESSION_BYTE_SIZE
                  + AWSSchemaRegistryConstants.SCHEMA_VERSION_ID_SIZE)

// COMMAND ----------

// MAGIC %md
// MAGIC ## ReadStream from Kafka

// COMMAND ----------

val startingOffsets = "latest"
val msk_df = spark.readStream
  .format("kafka")
  .option("kafka.bootstrap.servers", kafka_bootstrap_servers_plaintext) 
  .option("subscribe", topic)
  .option("checkpointLocation", kafkaCheckPointPath)
  .option("startingOffsets", startingOffsets)
  .load()
  .withColumn("schemaBinary", substring($"value", 0, headerSize))
  .withColumn("payload", $"value".substr(lit(headerSize + 1), length($"value")- headerSize))

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC * When you read from Kafka, two columns: 1. value (payload), 2. key - might have topics, partitions etc
// MAGIC * Most times key doesn't matter that much. E.g. write everything in the same topic, and then write the info on the schema on the key
// MAGIC * AWS Glue doesn't quite work that way - they embed the value / magic bytes in front of the payloads to specify the ID of the schema
// MAGIC * We're reading the encoded payload, need to use that schema ID to get the actual schema from Glue and then decode/parse
// MAGIC * Payload is Avro based (so we use .fromAvro)

// COMMAND ----------

// MAGIC %md
// MAGIC ## Set up the client for the Schema Registry

// COMMAND ----------

/**
configure the glueSchemaRegistryDeserializationFacade following this https://github.com/awslabs/aws-glue-schema-registry/blob/master/serializer-deserializer/src/main/java/com/amazonaws/services/schemaregistry/deserializers/GlueSchemaRegistryDeserializationFacade.java
the constructor we use is
     * @param properties           configuration properties
     * @param credentialProvider   credentials provider for integrating with schema
     *                             registry service - reuse cluster instance profile

*/

val provider: AwsCredentialsProvider = InstanceProfileCredentialsProvider.builder().build()
import software.amazon.awssdk.services.glue.model.Compatibility;

val properties = new Properties
properties.put(AWSSchemaRegistryConstants.AWS_REGION, "<AWS-REGION-FOR-GLUE-SCHEMA-REGISTRY>")
properties.put(AWSSchemaRegistryConstants.REGISTRY_NAME, "<AWS-GLUE-REGISTRY-NAME>")
properties.put(AWSSchemaRegistryConstants.SCHEMA_NAME, "<AWS-GLUE-SCHEMA-NAME>")

val schemaRegistryConfig = new GlueSchemaRegistryConfiguration(properties)
val glueSchemaRegistryDeserializationFacade = new GlueSchemaRegistryDeserializationFacade(schemaRegistryConfig, provider)

// COMMAND ----------

// MAGIC %md
// MAGIC ### Write out data from the stream using ForeachBatch

// COMMAND ----------

// Write out the parsed data to a Delta table
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.streaming.Trigger

// This method will pull the set of schemas needed to process each micro-batch.  It will pull the schemas each time a micro-batch is processed.
// This example will handle parsing rows with different schemas - it uses the schema ID to pull the correct schema for the value for each set of rows
// In this example the key is a string.  Keys can also be encoded in Avro format - in this case the below can be turned into a nested for loop where the
// schemas for the keys and values can be pulled from the registry and parsed

// To extend this further, you can use another attribute of each row to filter on so that different types of data that came in on the same topic can be identified, then 
// after parsing, write each set of data out to the correct Delta table
// You can also manipulate and transform the data more before writing it out.  
// In any case it is prudent to write a form of the data out that is as raw as possible to facilitate reprocessing.

msk_df.writeStream
  .foreachBatch {(df: DataFrame, epoch_id: Long) =>
    // For each batch we make a call to the Schema registry
    // Cache this so that when the logic below uses the Dataframe more than once the data is not pulled from the topic again
    val cacheDf = df.cache
    // Set the option for what to do with corrupt data in from_avro - either stop on the first failure it finds (FAILFAST) or just set corrupt data to null (PERMISSIVE)
    val fromAvroOptions = new java.util.HashMap[String, String]()
    //fromAvroOptions.put("mode", "PERMISSIVE")
    fromAvroOptions.put("mode", "FAILFAST")
    
    // Function that will fetch a schema from the schema registry by ID
    def getSchema(schemaBinary: Array[Byte]): String = {
      glueSchemaRegistryDeserializationFacade.getSchemaDefinition(schemaBinary)
    }

    // Fetch the distinct set of value Schema IDs that are in this set of data
    val distinctValueSchemaIdDF = cacheDf.select("schemaBinary").distinct() // Could be that you have overlapping schemas

    // For each valueSchemaId get the schemas from the schema registry
    for (valueRow <- distinctValueSchemaIdDF.collect) {
      // Pull the schema for this schema ID
      val currentValueSchemaId = sc.broadcast(valueRow.getAs[Array[Byte]]("schemaBinary")) //everything that is from the same schema goes to the same table
      val currentValueSchema = sc.broadcast(getSchema(currentValueSchemaId.value))
    
      // Filter the batch to the rows that have this value schema ID
      val filterValueDF = cacheDf.filter($"schemaBinary" === currentValueSchemaId.value)
        
      // Then for all the rows in the dataset with that value schema ID, parse the Avro data with the correct schema and write the micro-batch to a Delta table
      // In this example mergeSchema is set to true to enable schema evolution.  Set to false (which is the default) to prevent schema changes on the Delta table

      val parsedDf = filterValueDF
        .select(
          $"schemaBinary",
          $"topic",
          $"partition",
          $"offset",
          $"timestamp",
          $"timestampType",
          $"key",
          from_avro($"payload", currentValueSchema.value, fromAvroOptions).as('parsedValue)
        )
          
      parsedDf
        .write
        .format("delta")
        .mode("append")
        .option("mergeSchema", "true")
        .saveAsTable("raw_events")
    }
  }
  
  .trigger(Trigger.Once)
  .queryName("kafkaIngestionTestMSK")
  .option("checkpointLocation", tableCheckPointPath)
  .start()

// COMMAND ----------

