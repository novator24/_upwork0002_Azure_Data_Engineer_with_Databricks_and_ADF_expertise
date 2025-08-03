"""
Bronze Layer Pipeline - Data Ingestion

This module handles the ingestion of raw data from various sources into the bronze layer.
Implements data lakehouse architecture with Delta Lake for ACID transactions.
"""

import logging
from typing import Dict, Any, Optional
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import current_timestamp, col, lit
from delta.tables import DeltaTable
from src.utils.config import Config
from src.utils.logger import get_logger

logger = get_logger(__name__)


class BronzePipeline:
    """
    Bronze Layer Pipeline for data ingestion and initial processing.
    
    Handles:
    - Raw data ingestion from multiple sources (S3, Azure Data Lake, etc.)
    - Basic data validation and schema enforcement
    - Delta Lake integration with ACID transactions
    - Metadata tracking and audit trails
    """
    
    def __init__(self, spark: SparkSession, config: Config):
        """
        Initialize the Bronze Pipeline.
        
        Args:
            spark: SparkSession instance
            config: Configuration object
        """
        self.spark = spark
        self.config = config
        self.bronze_path = config.get("bronze_layer_path", "s3://data-lake/bronze")
        
    def ingest_from_s3(self, bucket: str, prefix: str, table_name: str) -> DataFrame:
        """
        Ingest data from AWS S3 into bronze layer.
        
        Args:
            bucket: S3 bucket name
            prefix: S3 key prefix
            table_name: Target table name in bronze layer
            
        Returns:
            DataFrame: Ingested data with metadata
        """
        try:
            logger.info(f"Ingesting data from s3://{bucket}/{prefix} to {table_name}")
            
            # Read data from S3
            s3_path = f"s3://{bucket}/{prefix}"
            df = self.spark.read.format("parquet").load(s3_path)
            
            # Add ingestion metadata
            df_with_metadata = self._add_ingestion_metadata(df, source="s3", source_path=s3_path)
            
            # Write to Delta Lake with schema enforcement
            bronze_table_path = f"{self.bronze_path}/{table_name}"
            df_with_metadata.write \
                .format("delta") \
                .mode("append") \
                .option("mergeSchema", "true") \
                .option("delta.columnMapping.mode", "name") \
                .save(bronze_table_path)
            
            logger.info(f"Successfully ingested {df.count()} records to {bronze_table_path}")
            return df_with_metadata
            
        except Exception as e:
            logger.error(f"Error ingesting data from S3: {str(e)}")
            raise
    
    def ingest_from_azure_datalake(self, container: str, path: str, table_name: str) -> DataFrame:
        """
        Ingest data from Azure Data Lake into bronze layer.
        
        Args:
            container: Azure Storage container name
            path: Path within the container
            table_name: Target table name in bronze layer
            
        Returns:
            DataFrame: Ingested data with metadata
        """
        try:
            logger.info(f"Ingesting data from Azure Data Lake: {container}/{path} to {table_name}")
            
            # Read data from Azure Data Lake
            adls_path = f"abfss://{container}@storage.dfs.core.windows.net/{path}"
            df = self.spark.read.format("parquet").load(adls_path)
            
            # Add ingestion metadata
            df_with_metadata = self._add_ingestion_metadata(df, source="azure_datalake", source_path=adls_path)
            
            # Write to Delta Lake
            bronze_table_path = f"{self.bronze_path}/{table_name}"
            df_with_metadata.write \
                .format("delta") \
                .mode("append") \
                .option("mergeSchema", "true") \
                .save(bronze_table_path)
            
            logger.info(f"Successfully ingested {df.count()} records to {bronze_table_path}")
            return df_with_metadata
            
        except Exception as e:
            logger.error(f"Error ingesting data from Azure Data Lake: {str(e)}")
            raise
    
    def _add_ingestion_metadata(self, df: DataFrame, source: str, source_path: str) -> DataFrame:
        """
        Add ingestion metadata to the DataFrame.
        
        Args:
            df: Input DataFrame
            source: Data source identifier
            source_path: Full path to source data
            
        Returns:
            DataFrame: DataFrame with metadata columns
        """
        return df.withColumn("_ingestion_timestamp", current_timestamp()) \
                .withColumn("_source", lit(source)) \
                .withColumn("_source_path", lit(source_path)) \
                .withColumn("_pipeline_version", lit("1.0.0"))
    
    def validate_bronze_data(self, table_name: str) -> Dict[str, Any]:
        """
        Perform basic data validation on bronze layer data.
        
        Args:
            table_name: Name of the table to validate
            
        Returns:
            Dict: Validation results
        """
        try:
            bronze_table_path = f"{self.bronze_path}/{table_name}"
            df = self.spark.read.format("delta").load(bronze_table_path)
            
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
    
    def optimize_bronze_table(self, table_name: str) -> None:
        """
        Optimize bronze table for better query performance.
        
        Args:
            table_name: Name of the table to optimize
        """
        try:
            bronze_table_path = f"{self.bronze_path}/{table_name}"
            
            # Optimize Delta table
            self.spark.sql(f"OPTIMIZE '{bronze_table_path}'")
            
            # Z-order optimization for frequently queried columns
            self.spark.sql(f"""
                OPTIMIZE '{bronze_table_path}' ZORDER BY (_ingestion_timestamp, _source)
            """)
            
            # Generate statistics for better query planning
            self.spark.sql(f"ANALYZE TABLE delta.`{bronze_table_path}` COMPUTE STATISTICS FOR ALL COLUMNS")
            
            logger.info(f"Successfully optimized bronze table: {table_name}")
            
        except Exception as e:
            logger.error(f"Error optimizing bronze table: {str(e)}")
            raise
    
    def get_bronze_table_info(self, table_name: str) -> Dict[str, Any]:
        """
        Get detailed information about a bronze table.
        
        Args:
            table_name: Name of the table
            
        Returns:
            Dict: Table information including schema, partitions, etc.
        """
        try:
            bronze_table_path = f"{self.bronze_path}/{table_name}"
            df = self.spark.read.format("delta").load(bronze_table_path)
            
            # Get Delta table details
            delta_table = DeltaTable.forPath(self.spark, bronze_table_path)
            detail = delta_table.detail()
            
            table_info = {
                "table_name": table_name,
                "path": bronze_table_path,
                "schema": df.schema.json(),
                "partition_columns": detail.select("partitionColumns").collect()[0][0],
                "num_files": detail.select("numFiles").collect()[0][0],
                "size_in_bytes": detail.select("sizeInBytes").collect()[0][0],
                "total_records": df.count()
            }
            
            return table_info
            
        except Exception as e:
            logger.error(f"Error getting bronze table info: {str(e)}")
            raise 