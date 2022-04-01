# Databricks notebook source

import uuid

from confluent_kafka import SerializingProducer
from confluent_kafka.serialization import StringSerializer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer

confluent_key = dbutils.secrets.get("<SECRET-SCOPE>", "<SECRET-NAME>")
confluent_secret = dbutils.secrets.get("<SECRET-SCOPE>", "<SECRET-NAME>")
confluent_sr_key = dbutils.secrets.get("<SECRET-SCOPE>", "<SECRET-NAME>")
confluent_sr_secret = dbutils.secrets.get("<SECRET-SCOPE>", "<SECRET-NAME>")
confluent_output_topic = "<CONFLUENT-KAFKA-TOPIC-NAME>"
checkpoint_path = "<CHECKPOINT-PATH>"

# Required connection configs for Kafka producer, consumer, and admin

bootstrap_servers = "<KAFKA-BOOTSTRAP-SERVERS>"
security_protocol = "SASL_SSL"
sasl_mechanisms = "PLAIN"
sasl_username = confluent_key
sasl_password = confluent_secret

# Best practice for higher availability in librdkafka clients prior to 1.7
session_timeout=45000

# Required connection configs for Confluent Cloud Schema Registry

confluent_schema_registry_conf = {
  "url": "<CONFLUENT-SCHEMA-REGISTRY-URL>",
  "basic.auth.user.info" : f"{confluent_sr_key}:{confluent_sr_secret}"
}

schema_registry_client = SchemaRegistryClient(confluent_schema_registry_conf)

# COMMAND ----------

# Convert schemas to binary format

def generate_binary_schema(
  schema_path,
  binary_extension = "bin"
):

  full_path_binary = f"{schema_path}.{binary_extension}"
  with open(schema_path, 'r') as source_schema:
    with open(full_path_binary, "wb") as binary_schema:
      writer = DataFileWriter(binary_schema, DatumWriter(), source_schema.read())
      writer.close()

  return full_path_binary

schema_file_v1 = "<DBFS-PATH-TO-AVRO-SCHEMA-V1>"
schema_file_v2 = "<DBFS-PATH-TO-AVRO-SCHEMA-V2>"
binary_schema_list = []

for schema in [schema_file_v1, schema_file_v2]:
  binary_schema = generate_binary_schema(schema)
  binary_schema_list.append(binary_schema)
  
# Define sample payloads

keys_schema_v1 = {
  "schema_path": binary_schema[0],
  "payload": {
    "productId": [str(uuid.uuid4()) for _ in range(0,3)],
    "type": [
      "shirt",
      "pants",
      "shoes",
    ]
  }
}

additional_keys = {
  "color": ["black", "blue", "red"],
  "size": ["xs", "s", "m", "l", "xl"]
}

payload_v2 = {**keys_schema_v1["payload"], **additional_keys}
keys_schema_v2 = {
  "schema_path": binary_schema[1],
  "payload": payload_v2
}
candidate_payloads = [keys_schema_v1, keys_schema_v2]

# COMMAND ----------

kafka_config_write = {
  # Required connection configs for Kafka producer, consumer, and admin
  "bootstrap.servers": bootstrap_servers,
  "security.protocol": "SASL_SSL",
  "sasl.mechanisms": "PLAIN",
  "sasl.username": confluent_key,
  "sasl.password": confluent_secret
}

# COMMAND ----------

import copy
import json
import time
import random

from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka import avro
from avro.datafile import DataFileReader, DataFileWriter
from avro.io import DatumReader, DatumWriter


def send_record(
  topic,
  schema_path,
  record_key,
  record_value
):

  # Read data from an avro file
  with open(schema_path, 'rb') as f:
    reader = DataFileReader(f, DatumReader())
    metadata = copy.deepcopy(reader.meta)
    schema_from_file = json.dumps(json.loads(metadata['avro.schema']))
    
  serializer = AvroSerializer(
    schema_registry_client = schema_registry_client,
    schema_str = schema_from_file
  )
    
  kafka_config_write["key.serializer"] = StringSerializer('utf_8')
  kafka_config_write["value.serializer"] = serializer
  
  producer = SerializingProducer(kafka_config_write)

  try:

    producer.produce(
      topic = topic,
      key = record_key,
      value = record_value
    )
    
  except Exception as e:
    print(f"Error while producing record value - {record_value} to topic - {topic}: {e}")
  else:
    print(f"Producing record value - {record_value} to topic - {topic}")

  producer.flush()
  
def produce_random(
  candidate_payloads,
  topic,
  quantity: int = 1000,
  interval: int = 5,
):
  
  for index in range(0, quantity):
    
    event = {}
    event["eventId"] = str(uuid.uuid4())
    event["timestamp"] = int(time.time())
    payload_index = random.randint(0, len(candidate_payloads)-1)
    keys = candidate_payloads[payload_index]
    schema_path = keys["schema_path"]
    payload = keys["payload"]
      
    for key, value in payload.items():
      index = random.randint(0, len(value) - 1)
      event[key] = value[index]
      
    send_record(
      topic = topic,
      schema_path = schema_path,
      record_key = str(random.randint(0, 3)), #will be used to distribute events across partitions
      record_value = event
    )
    
    time.sleep(interval)

produce_random(
  candidate_payloads = candidate_payloads,
  quantity = 1000,
  interval = 5,
  topic = confluent_output_topic
)

# COMMAND ----------


