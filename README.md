# Databricks End-to-End Kafka Streaming Examples
## Evolving Avro Schemas with Schema Registry Integration

The code in this repo can be used as a starting point for integrating with different types of Kafka PaaS offerings:

* AWS MSK
* Confluent Kafka

The ingestion notebooks for each platform have their own particularities due to the different libraries used for integrating with the Schema Registry (AWS Glue for MSK and Confluent Schema Registry for Confluent Kafka). There are also separate notebooks for Confluent and MSK which act as random event producers in order to facilitate simulations.

### Requirements

* An AWS MSK or Confluent Kafka broker
    * Make sure to store the relevant configurations and authentication info in Databricks Secret Scopes
    * For AWS MSK, make sure that there is an instance profile attached to the cluster which gives the necessary permissions to integrate with MSK and Glue
* Maven Dependencies
    * For AWS MSK:
        * org.jetbrains.kotlinx:kotlinx-coroutines-core:1.5.2
        * software.amazon.glue:schema-registry-serde:1.1.4
        * software.amazon.glue:schema-registry-common:1.1.4
        * org.apache.kafka:kafka-clients:3.0.0
    * For Confluent:
        * [kafka_schema_registry_client_5_3_1.jar](https://mvnrepository.com/artifact/io.confluent/kafka-schema-registry-client/5.3.1)
        * [kafka_clients_2_6_0.jar](https://mvnrepository.com/artifact/org.apache.kafka/kafka-clients/2.6.0)
        * It is **really** important that these exact versions are used, otherwise it won't work.
    * Highly recommended to upload the jar files into DBFS and install from there, so that they can be quickly installed when job clusters are created