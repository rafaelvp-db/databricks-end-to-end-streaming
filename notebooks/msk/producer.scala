// Databricks notebook source
// MAGIC %md
// MAGIC ## End-to-End Kafka Example
// MAGIC ### Streaming Kafka Events from MSK into Delta Lake

// COMMAND ----------

import org.apache.kafka.clients.producer.{KafkaProducer,ProducerConfig,ProducerRecord}
import org.apache.kafka.common.serialization.StringSerializer
import com.amazonaws.services.schemaregistry.serializers.avro.AWSKafkaAvroSerializer
import com.amazonaws.services.schemaregistry.utils.{AvroRecordType,AWSSchemaRegistryConstants}
import org.apache.kafka.common.errors.SerializationException
import org.apache.avro.generic.GenericData
import org.apache.avro.generic.GenericRecord
import org.apache.avro.Schema
import org.apache.avro.Schema.Parser
import java.util.Properties
import java.io.{IOException,FileInputStream,InputStream,File}

// COMMAND ----------

// DBTITLE 1,Specifying the path to our Avro Schemas
val schemaFileV1 = "<AVRO-SCHEMA-PATH-V1>"
val schemaFileV2 = "<AVRO-SCHEMA-PATH-V1>"

// COMMAND ----------

// get secret credentials
val kafka_bootstrap_servers_plaintext = dbutils.secrets.get( "<SECRET-SCOPE>", "<SECRET-NAME-KAFKA-BOOTSTRAP-SERVERS>" )
val topic = "<TOPIC-NAME>"

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC ### Configuring our Producer
// MAGIC 
// MAGIC We need to make sure our Kafka producer is properly configured. This includes:
// MAGIC 
// MAGIC <br/>
// MAGIC 
// MAGIC * Specifying our broker configurations
// MAGIC * Setting our default synchronous HTTP client to UrlConnectionHttpClient
// MAGIC * Specifying our AWS Glue Schema Registry parameters
// MAGIC * **AWS Glue Schema Registry** will automatically generate new schema versions as soon as it detects that the Avro Schema has evolved

// COMMAND ----------

import software.amazon.awssdk.services.glue.model.Compatibility;

val properties = new Properties
// Set the default synchronous HTTP client to UrlConnectionHttpClient
System.setProperty("software.amazon.awssdk.http.service.impl", "software.amazon.awssdk.http.urlconnection.UrlConnectionSdkHttpService")
properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka_bootstrap_servers_plaintext)
properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[AWSKafkaAvroSerializer].getName)
properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[AWSKafkaAvroSerializer].getName)
properties.put(AWSSchemaRegistryConstants.AWS_REGION, "<AWS-REGION-FOR-GLUE-SCHEMA-REGISTRY>")
properties.put(AWSSchemaRegistryConstants.REGISTRY_NAME, "<AWS-GLUE-REGISTRY-NAME>")
properties.put(AWSSchemaRegistryConstants.SCHEMA_NAME, "<AWS-GLUE-SCHEMA-NAME>")
properties.put(AWSSchemaRegistryConstants.COMPATIBILITY_SETTING, Compatibility.FULL)
properties.put(AWSSchemaRegistryConstants.SCHEMA_AUTO_REGISTRATION_SETTING, true:java.lang.Boolean)

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC ### Sample Payloads
// MAGIC 
// MAGIC We will generate random events from both Avro Schemas that are available. Messages conforming with each of the schemas will have random values for the keys belonging to that schema - we just need to define which are the possible values that we want to include in our payloads.

// COMMAND ----------

// DBTITLE 1,Sample Payloads
//define our sample data - two schemas

val keys_schema_v1 = Map(
  "productId" -> Array(
    "7c9bdf04d36248bcb11d535e8bda35d9",
    "2c4bdf04d36248bcb11d535e8bda35d1",
    "9c1bdf04d36248bcb11d535e8bda35d2",
  ),
  "type" -> Array(
    "shirt",
    "pants",
    "shoes",
  )
)

val additional_keys = Map(
  "color" -> Array("black", "blue", "red"),
  "size" -> Array("xs", "s", "m", "l", "xl")
)

var keys_schema_v2 = keys_schema_v1.++(additional_keys)
val schema_events_v1:Schema = new Parser().parse(new File(schemaFileV1))
val schema_events_v2:Schema = new Parser().parse(new File(schemaFileV2))

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC ### Simulating our Producers
// MAGIC 
// MAGIC In the code below:
// MAGIC 
// MAGIC <br/>
// MAGIC 
// MAGIC * We implement *produceEvent*, the function which will create random events and publish them into our Kafka topic.
// MAGIC * We implement *simulateProduceEvents*, the function which will simulate event production by randomly generating payloads conforming to our schemas V1 and V2
// MAGIC * We can specify the quantity of events (*eventQuantity*) as well as the interval between each of them (*interval*)
// MAGIC 
// MAGIC <br/>
// MAGIC 
// MAGIC ### Simulation Scenario
// MAGIC 
// MAGIC <br/>
// MAGIC 
// MAGIC We will simulate a hypothetical scenario, where:
// MAGIC 
// MAGIC <br/>
// MAGIC 
// MAGIC * 30% of our users have a legacy version of our Mobile App, hence they are still producing events with V1 of our schema;
// MAGIC * The remaining 70% of our users have a recent version of our App, meaning they are producing events with V2 of our schema;
// MAGIC * In V1 of our Avro Schema, we only have **productId** and **type**
// MAGIC * In V2 of our Avro Schema, in addition to the fields in V1, we also have **size** and **color**

// COMMAND ----------

import scala.util.Random
import java.util.UUID

def produceEvent(schema: Schema, keys: Map[String, Array[String]]): Unit = {
  
  val event:GenericRecord = new GenericData.Record(schema)
  val producer = new KafkaProducer[String, GenericRecord](properties)

  try {
      
    val record = new ProducerRecord[String, GenericRecord](topic, event)
    
    for ((k,v) <- keys) {
      printf("key: %s, value: %s\n", k, v)
      val random = new Random
      val value = v(
        random.nextInt(v.length)
      )
      event.put(k, value)
    }
    
    event.put("timestamp", System.currentTimeMillis / 1000)
    event.put("eventId", UUID.randomUUID().toString())
    println(event)
    producer.send(record)
    producer.flush()
    println("Sent message")

  } catch {
    case e: InterruptedException => println(e.getStackTrace)
  }
  
}

def simulateProduceEvents(eventQuantity: Int, interval: Int): Unit = {
  
  var counter = 0
  while (counter < eventQuantity) {
    
    val (schema, keys) = (
      if (counter % 3 == 0) (schema_events_v1, keys_schema_v1)
      else (schema_events_v2, keys_schema_v2)
    )
    produceEvent(schema, keys)
    Thread.sleep(interval)
    counter += 1
  }
}

simulateProduceEvents(3600, 1000)