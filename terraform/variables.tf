variable "bronze_source_table" {
  type = string
  default = "raw_table"
}

variable "bronze_target_table" {
  type = string
  default = "bronze_table"
}

variable "silver_source_table" {
  type = string
  default = "bronze_table"
}

variable "silver_target_table" {
  type = string
  default = "silver_table"
}

variable "gold_source_table" {
  type = string
  default = "silver_table"
}

variable "gold_target_table" {
  type = string
  default = "gold_table"
}