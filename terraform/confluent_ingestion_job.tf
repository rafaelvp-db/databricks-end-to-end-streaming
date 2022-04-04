
terraform {
  required_providers {
    databricks = {
      source = "databrickslabs/databricks"
      version = "0.5.4"
    }
  }
}

provider "databricks" {
}

data "databricks_current_user" "me" {}
data "databricks_spark_version" "latest" {}
data "databricks_node_type" "smallest" {
  local_disk = true
}

resource "databricks_job" "this" {
  name = "Confluent Ingestion Job"

  job_cluster {
    job_cluster_key = "job_cluster"
    new_cluster {
      num_workers   = 5
      spark_version = data.databricks_spark_version.latest.id
      node_type_id  = data.databricks_node_type.smallest.id
    }
  }

  task {
    task_key = "confluent_ingest"
    job_cluster_key = "job_cluster"

    notebook_task {
      notebook_path = "/dbfs/tmp/ingest.py"
    }
    library {
      jar = "dbfs:/FileStore/jars/kafka_schema_registry_client_5_3_1.jar"
    }
    library {
      jar = "dbfs:/FileStore/jars/kafka_clients_2_6_0.jar"
    }
  }

  task {
    task_key = "bronze"
    job_cluster_key = "job_cluster"

    depends_on {
      task_key = "confluent_ingest"
    }

    notebook_task {
      notebook_path = "/dbfs/tmp/bronze.py"
      base_parameters = {
          source_table = "raw_events"
          target_table = "bronze_table"
      }
    }
  }

  task {
    task_key = "silver"
    job_cluster_key = "job_cluster"

    depends_on {
      task_key = "bronze"
    }

    notebook_task {
      notebook_path = "/dbfs/tmp/silver.py"
      base_parameters = {
          source_table = "bronze_table"
          target_table = "silver_table"
      }
    }
  }

  task {
    task_key = "gold"
    job_cluster_key = "job_cluster"

    depends_on {
      task_key = "silver"
    }

    notebook_task {
      notebook_path = "/dbfs/tmp/gold.py"
      base_parameters = {
          source_table = "silver_table"
          target_table = "gold_table"
      }
    }
  }
}