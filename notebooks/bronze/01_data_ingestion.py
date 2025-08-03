# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze Layer - Data Ingestion
# MAGIC 
# MAGIC This notebook handles the ingestion of raw data from various sources into the bronze layer.
# MAGIC 
# MAGIC ## Overview
# MAGIC - Ingest data from AWS S3 and Azure Data Lake
# MAGIC - Apply basic validation and schema enforcement
# MAGIC - Store in Delta Lake format for ACID transactions
# MAGIC - Add metadata tracking and audit trails

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup and Configuration

# COMMAND ----------

# Import required libraries
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, col, lit
from delta.tables import DeltaTable
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Initialize Bronze Pipeline

# COMMAND ----------

# Bronze layer configuration
bronze_path = "s3://data-lake/bronze"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Ingest Customer Data from S3

# COMMAND ----------

def ingest_customer_data_from_s3(bucket: str, prefix: str, table_name: str):
    """Ingest customer data from AWS S3 into bronze layer."""
    
    try:
        logger.info(f"Ingesting customer data from s3://{bucket}/{prefix}")
        
        # Read data from S3
        s3_path = f"s3://{bucket}/{prefix}"
        df = spark.read.format("parquet").load(s3_path)
        
        # Add ingestion metadata
        df_with_metadata = df.withColumn("_ingestion_timestamp", current_timestamp()) \
                             .withColumn("_source", lit("s3")) \
                             .withColumn("_source_path", lit(s3_path)) \
                             .withColumn("_pipeline_version", lit("1.0.0"))
        
        # Write to Delta Lake
        bronze_table_path = f"{bronze_path}/{table_name}"
        df_with_metadata.write \
            .format("delta") \
            .mode("append") \
            .option("mergeSchema", "true") \
            .option("delta.columnMapping.mode", "name") \
            .save(bronze_table_path)
        
        logger.info(f"Successfully ingested {df.count()} customer records to {bronze_table_path}")
        return df_with_metadata
        
    except Exception as e:
        logger.error(f"Error ingesting customer data from S3: {str(e)}")
        raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## Ingest Sales Data from Azure Data Lake

# COMMAND ----------

def ingest_sales_data_from_azure(container: str, path: str, table_name: str):
    """Ingest sales data from Azure Data Lake into bronze layer."""
    
    try:
        logger.info(f"Ingesting sales data from Azure Data Lake: {container}/{path}")
        
        # Read data from Azure Data Lake
        adls_path = f"abfss://{container}@storage.dfs.core.windows.net/{path}"
        df = spark.read.format("parquet").load(adls_path)
        
        # Add ingestion metadata
        df_with_metadata = df.withColumn("_ingestion_timestamp", current_timestamp()) \
                             .withColumn("_source", lit("azure_datalake")) \
                             .withColumn("_source_path", lit(adls_path)) \
                             .withColumn("_pipeline_version", lit("1.0.0"))
        
        # Write to Delta Lake
        bronze_table_path = f"{bronze_path}/{table_name}"
        df_with_metadata.write \
            .format("delta") \
            .mode("append") \
            .option("mergeSchema", "true") \
            .save(bronze_table_path)
        
        logger.info(f"Successfully ingested {df.count()} sales records to {bronze_table_path}")
        return df_with_metadata
        
    except Exception as e:
        logger.error(f"Error ingesting sales data from Azure: {str(e)}")
        raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Validation

# COMMAND ----------

def validate_bronze_data(table_name: str):
    """Perform basic data validation on bronze layer data."""
    
    try:
        bronze_table_path = f"{bronze_path}/{table_name}"
        df = spark.read.format("delta").load(bronze_table_path)
        
        validation_results = {
            "table_name": table_name,
            "total_records": df.count(),
            "null_counts": {},
            "duplicate_records": 0,
            "schema_validation": "PASS"
        }
        
        # Check for null values in key columns
        for column in df.columns:
            if not column.startswith("_"):  # Skip metadata columns
                null_count = df.filter(col(column).isNull()).count()
                validation_results["null_counts"][column] = null_count
        
        # Check for duplicate records (excluding metadata columns)
        data_columns = [col for col in df.columns if not col.startswith("_")]
        if data_columns:
            duplicate_count = df.groupBy(data_columns).count().filter("count > 1").count()
            validation_results["duplicate_records"] = duplicate_count
        
        logger.info(f"Validation completed for {table_name}: {validation_results}")
        return validation_results
        
    except Exception as e:
        logger.error(f"Error validating bronze data: {str(e)}")
        raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## Table Optimization

# COMMAND ----------

def optimize_bronze_table(table_name: str):
    """Optimize bronze table for better query performance."""
    
    try:
        bronze_table_path = f"{bronze_path}/{table_name}"
        
        # Optimize Delta table
        spark.sql(f"OPTIMIZE '{bronze_table_path}'")
        
        # Z-order optimization for frequently queried columns
        spark.sql(f"""
            OPTIMIZE '{bronze_table_path}' ZORDER BY (_ingestion_timestamp, _source)
        """)
        
        # Generate statistics for better query planning
        spark.sql(f"ANALYZE TABLE delta.`{bronze_table_path}` COMPUTE STATISTICS FOR ALL COLUMNS")
        
        logger.info(f"Successfully optimized bronze table: {table_name}")
        
    except Exception as e:
        logger.error(f"Error optimizing bronze table: {str(e)}")
        raise

# COMMAND ----------

# MAGIC %md
# MAGIC ## Execute Bronze Layer Pipeline

# COMMAND ----------

# Execute the bronze layer pipeline
try:
    logger.info("Starting bronze layer pipeline execution")
    
    # Ingest customer data from S3
    customer_df = ingest_customer_data_from_s3(
        bucket="data-lake-bucket",
        prefix="raw/customers",
        table_name="customers"
    )
    
    # Ingest sales data from Azure Data Lake
    sales_df = ingest_sales_data_from_azure(
        container="data-lake-container",
        path="raw/sales",
        table_name="sales"
    )
    
    # Validate ingested data
    customer_validation = validate_bronze_data("customers")
    sales_validation = validate_bronze_data("sales")
    
    # Optimize tables
    optimize_bronze_table("customers")
    optimize_bronze_table("sales")
    
    logger.info("Bronze layer pipeline completed successfully")
    
except Exception as e:
    logger.error(f"Bronze layer pipeline failed: {str(e)}")
    raise 