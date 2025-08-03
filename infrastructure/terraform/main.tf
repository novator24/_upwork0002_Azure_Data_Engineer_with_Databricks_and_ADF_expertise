# Databricks Data Engineering Demo - Infrastructure as Code
# This Terraform configuration sets up the Databricks workspace and related resources

terraform {
  required_version = ">= 1.0"
  required_providers {
    azurerm = {
      source  = "hashicorp/azurerm"
      version = "~> 3.0"
    }
    databricks = {
      source  = "databricks/databricks"
      version = "~> 1.0"
    }
  }
}

# Configure providers
provider "azurerm" {
  features {}
}

provider "databricks" {
  host = azurerm_databricks_workspace.workspace.workspace_url
}

# Resource group
resource "azurerm_resource_group" "rg" {
  name     = "databricks-data-engineering-demo"
  location = "East US"
  
  tags = {
    Environment = "Demo"
    Project     = "Data Engineering"
  }
}

# Storage account for data lake
resource "azurerm_storage_account" "data_lake" {
  name                     = "databricksdatalake${random_string.suffix.result}"
  resource_group_name      = azurerm_resource_group.rg.name
  location                 = azurerm_resource_group.rg.location
  account_tier             = "Standard"
  account_replication_type = "LRS"
  account_kind             = "StorageV2"
  is_hns_enabled          = true
  
  tags = {
    Environment = "Demo"
    Project     = "Data Engineering"
  }
}

# Data lake containers
resource "azurerm_storage_container" "bronze" {
  name                  = "bronze"
  storage_account_name  = azurerm_storage_account.data_lake.name
  container_access_type = "private"
}

resource "azurerm_storage_container" "silver" {
  name                  = "silver"
  storage_account_name  = azurerm_storage_account.data_lake.name
  container_access_type = "private"
}

resource "azurerm_storage_container" "gold" {
  name                  = "gold"
  storage_account_name  = azurerm_storage_account.data_lake.name
  container_access_type = "private"
}

# Databricks workspace
resource "azurerm_databricks_workspace" "workspace" {
  name                = "databricks-data-engineering-demo"
  resource_group_name = azurerm_resource_group.rg.name
  location            = azurerm_resource_group.rg.location
  sku                 = "standard"
  
  tags = {
    Environment = "Demo"
    Project     = "Data Engineering"
  }
}

# Databricks cluster for data processing
resource "databricks_cluster" "data_processing" {
  cluster_name = "data-processing-cluster"
  
  spark_version = "11.3.x-scala2.12"
  node_type_id  = "Standard_DS3_v2"
  
  autoscale {
    min_workers = 1
    max_workers = 3
  }
  
  autotermination_minutes = 20
  
  spark_conf = {
    "spark.sql.adaptive.enabled"                    = "true"
    "spark.sql.adaptive.coalescePartitions.enabled" = "true"
    "spark.sql.adaptive.skewJoin.enabled"          = "true"
    "spark.sql.extensions"                          = "io.delta.sql.DeltaSparkSessionExtension"
    "spark.sql.catalog.spark_catalog"               = "org.apache.spark.sql.delta.catalog.DeltaCatalog"
  }
  
  tags = {
    Environment = "Demo"
    Project     = "Data Engineering"
  }
}

# Databricks job for pipeline orchestration
resource "databricks_job" "data_pipeline" {
  name = "Data Engineering Pipeline"
  
  new_cluster {
    spark_version = "11.3.x-scala2.12"
    node_type_id  = "Standard_DS3_v2"
    
    autoscale {
      min_workers = 1
      max_workers = 3
    }
    
    autotermination_minutes = 20
    
    spark_conf = {
      "spark.sql.adaptive.enabled"                    = "true"
      "spark.sql.adaptive.coalescePartitions.enabled" = "true"
      "spark.sql.adaptive.skewJoin.enabled"          = "true"
      "spark.sql.extensions"                          = "io.delta.sql.DeltaSparkSessionExtension"
      "spark.sql.catalog.spark_catalog"               = "org.apache.spark.sql.delta.catalog.DeltaCatalog"
    }
  }
  
  notebook_task {
    notebook_path = "/Repos/Data Engineering Demo/notebooks/bronze/01_data_ingestion"
  }
  
  schedule {
    quartz_cron_expression = "0 0 2 * * ?"  # Daily at 2 AM
    timezone_id            = "UTC"
  }
  
  tags = {
    Environment = "Demo"
    Project     = "Data Engineering"
  }
}

# Random string for unique naming
resource "random_string" "suffix" {
  length  = 6
  special = false
  upper   = false
}

# Outputs
output "databricks_workspace_url" {
  value = azurerm_databricks_workspace.workspace.workspace_url
}

output "storage_account_name" {
  value = azurerm_storage_account.data_lake.name
}

output "data_lake_containers" {
  value = {
    bronze = azurerm_storage_container.bronze.name
    silver = azurerm_storage_container.silver.name
    gold   = azurerm_storage_container.gold.name
  }
} 