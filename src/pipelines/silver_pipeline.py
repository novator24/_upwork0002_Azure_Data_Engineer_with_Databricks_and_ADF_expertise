"""
Silver Layer Pipeline - Data Processing and Transformation

This module handles the transformation of bronze layer data into cleaned, 
business-ready datasets in the silver layer with data quality checks.
"""

import logging
from typing import Dict, Any, List, Optional
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, when, upper, lower, trim, regexp_replace, 
    current_timestamp, lit, coalesce, to_date, to_timestamp
)
from pyspark.sql.types import StructType, StructField, StringType, TimestampType
from delta.tables import DeltaTable
from src.utils.config import Config
from src.utils.logger import get_logger
from src.utils.data_quality import DataQualityChecker

logger = get_logger(__name__)


class SilverPipeline:
    """
    Silver Layer Pipeline for data processing and transformation.
    
    Handles:
    - Data cleaning and standardization
    - Business rule application
    - Data quality validation
    - Schema evolution and optimization
    - Performance tuning for query optimization
    """
    
    def __init__(self, spark: SparkSession, config: Config):
        """
        Initialize the Silver Pipeline.
        
        Args:
            spark: SparkSession instance
            config: Configuration object
        """
        self.spark = spark
        self.config = config
        self.bronze_path = config.get("bronze_layer_path", "s3://data-lake/bronze")
        self.silver_path = config.get("silver_layer_path", "s3://data-lake/silver")
        self.data_quality_checker = DataQualityChecker(spark, config)
        
    def process_customer_data(self, bronze_table: str, silver_table: str) -> DataFrame:
        """
        Process customer data from bronze to silver layer.
        
        Args:
            bronze_table: Source table name in bronze layer
            silver_table: Target table name in silver layer
            
        Returns:
            DataFrame: Processed customer data
        """
        try:
            logger.info(f"Processing customer data from {bronze_table} to {silver_table}")
            
            # Read from bronze layer
            bronze_table_path = f"{self.bronze_path}/{bronze_table}"
            df = self.spark.read.format("delta").load(bronze_table_path)
            
            # Apply data transformations
            processed_df = self._transform_customer_data(df)
            
            # Apply data quality checks
            quality_results = self.data_quality_checker.validate_customer_data(processed_df)
            logger.info(f"Data quality results: {quality_results}")
            
            # Write to silver layer
            silver_table_path = f"{self.silver_path}/{silver_table}"
            processed_df.write \
                .format("delta") \
                .mode("overwrite") \
                .option("mergeSchema", "true") \
                .option("delta.columnMapping.mode", "name") \
                .save(silver_table_path)
            
            logger.info(f"Successfully processed {processed_df.count()} customer records to {silver_table_path}")
            return processed_df
            
        except Exception as e:
            logger.error(f"Error processing customer data: {str(e)}")
            raise
    
    def process_sales_data(self, bronze_table: str, silver_table: str) -> DataFrame:
        """
        Process sales data from bronze to silver layer.
        
        Args:
            bronze_table: Source table name in bronze layer
            silver_table: Target table name in silver layer
            
        Returns:
            DataFrame: Processed sales data
        """
        try:
            logger.info(f"Processing sales data from {bronze_table} to {silver_table}")
            
            # Read from bronze layer
            bronze_table_path = f"{self.bronze_path}/{bronze_table}"
            df = self.spark.read.format("delta").load(bronze_table_path)
            
            # Apply data transformations
            processed_df = self._transform_sales_data(df)
            
            # Apply data quality checks
            quality_results = self.data_quality_checker.validate_sales_data(processed_df)
            logger.info(f"Data quality results: {quality_results}")
            
            # Write to silver layer
            silver_table_path = f"{self.silver_path}/{silver_table}"
            processed_df.write \
                .format("delta") \
                .mode("overwrite") \
                .option("mergeSchema", "true") \
                .save(silver_table_path)
            
            logger.info(f"Successfully processed {processed_df.count()} sales records to {silver_table_path}")
            return processed_df
            
        except Exception as e:
            logger.error(f"Error processing sales data: {str(e)}")
            raise
    
    def _transform_customer_data(self, df: DataFrame) -> DataFrame:
        """
        Apply business transformations to customer data.
        
        Args:
            df: Input customer DataFrame
            
        Returns:
            DataFrame: Transformed customer data
        """
        return df.select(
            # Standardize customer ID
            col("customer_id").cast("string").alias("customer_id"),
            
            # Clean and standardize names
            trim(upper(col("first_name"))).alias("first_name"),
            trim(upper(col("last_name"))).alias("last_name"),
            
            # Standardize email
            lower(trim(col("email"))).alias("email"),
            
            # Clean phone number
            regexp_replace(col("phone"), r"[^\d]", "").alias("phone"),
            
            # Standardize address
            trim(upper(col("address"))).alias("address"),
            trim(upper(col("city"))).alias("city"),
            trim(upper(col("state"))).alias("state"),
            trim(upper(col("zip_code"))).alias("zip_code"),
            
            # Standardize country
            when(col("country").isNull(), "US").otherwise(trim(upper(col("country")))).alias("country"),
            
            # Add processing metadata
            current_timestamp().alias("_processed_timestamp"),
            lit("silver_pipeline").alias("_processing_pipeline"),
            col("_ingestion_timestamp"),
            col("_source"),
            col("_source_path")
        )
    
    def _transform_sales_data(self, df: DataFrame) -> DataFrame:
        """
        Apply business transformations to sales data.
        
        Args:
            df: Input sales DataFrame
            
        Returns:
            DataFrame: Transformed sales data
        """
        return df.select(
            # Standardize order ID
            col("order_id").cast("string").alias("order_id"),
            col("customer_id").cast("string").alias("customer_id"),
            
            # Clean product information
            trim(upper(col("product_name"))).alias("product_name"),
            trim(upper(col("product_category"))).alias("product_category"),
            
            # Standardize monetary values
            col("unit_price").cast("decimal(10,2)").alias("unit_price"),
            col("quantity").cast("integer").alias("quantity"),
            (col("unit_price") * col("quantity")).alias("total_amount"),
            
            # Standardize dates
            to_date(col("order_date"), "yyyy-MM-dd").alias("order_date"),
            to_timestamp(col("order_timestamp")).alias("order_timestamp"),
            
            # Clean status
            trim(upper(col("order_status"))).alias("order_status"),
            
            # Add processing metadata
            current_timestamp().alias("_processed_timestamp"),
            lit("silver_pipeline").alias("_processing_pipeline"),
            col("_ingestion_timestamp"),
            col("_source"),
            col("_source_path")
        )
    
    def merge_silver_tables(self, table1: str, table2: str, merge_key: str, output_table: str) -> DataFrame:
        """
        Merge two silver tables based on a common key.
        
        Args:
            table1: First table name
            table2: Second table name
            merge_key: Column to merge on
            output_table: Output table name
            
        Returns:
            DataFrame: Merged data
        """
        try:
            logger.info(f"Merging tables {table1} and {table2} on key {merge_key}")
            
            # Read tables
            df1 = self.spark.read.format("delta").load(f"{self.silver_path}/{table1}")
            df2 = self.spark.read.format("delta").load(f"{self.silver_path}/{table2}")
            
            # Perform merge
            merged_df = df1.join(df2, on=merge_key, how="left")
            
            # Write merged data
            output_path = f"{self.silver_path}/{output_table}"
            merged_df.write \
                .format("delta") \
                .mode("overwrite") \
                .option("mergeSchema", "true") \
                .save(output_path)
            
            logger.info(f"Successfully merged tables into {output_path}")
            return merged_df
            
        except Exception as e:
            logger.error(f"Error merging silver tables: {str(e)}")
            raise
    
    def optimize_silver_table(self, table_name: str, partition_columns: List[str] = None) -> None:
        """
        Optimize silver table for better query performance.
        
        Args:
            table_name: Name of the table to optimize
            partition_columns: Columns to partition by
        """
        try:
            silver_table_path = f"{self.silver_path}/{table_name}"
            
            # Optimize Delta table
            self.spark.sql(f"OPTIMIZE '{silver_table_path}'")
            
            # Z-order optimization for frequently queried columns
            if partition_columns:
                zorder_columns = ", ".join(partition_columns)
                self.spark.sql(f"""
                    OPTIMIZE '{silver_table_path}' ZORDER BY ({zorder_columns})
                """)
            
            # Generate statistics for better query planning
            self.spark.sql(f"ANALYZE TABLE delta.`{silver_table_path}` COMPUTE STATISTICS FOR ALL COLUMNS")
            
            logger.info(f"Successfully optimized silver table: {table_name}")
            
        except Exception as e:
            logger.error(f"Error optimizing silver table: {str(e)}")
            raise
    
    def get_silver_table_stats(self, table_name: str) -> Dict[str, Any]:
        """
        Get statistics for a silver table.
        
        Args:
            table_name: Name of the table
            
        Returns:
            Dict: Table statistics
        """
        try:
            silver_table_path = f"{self.silver_path}/{table_name}"
            df = self.spark.read.format("delta").load(silver_table_path)
            
            # Get Delta table details
            delta_table = DeltaTable.forPath(self.spark, silver_table_path)
            detail = delta_table.detail()
            
            stats = {
                "table_name": table_name,
                "total_records": df.count(),
                "num_files": detail.select("numFiles").collect()[0][0],
                "size_in_bytes": detail.select("sizeInBytes").collect()[0][0],
                "partition_columns": detail.select("partitionColumns").collect()[0][0],
                "columns": df.columns
            }
            
            return stats
            
        except Exception as e:
            logger.error(f"Error getting silver table stats: {str(e)}")
            raise
    
    def vacuum_silver_table(self, table_name: str, retention_hours: int = 168) -> None:
        """
        Vacuum silver table to remove old files and optimize storage.
        
        Args:
            table_name: Name of the table to vacuum
            retention_hours: Hours to retain (default: 7 days)
        """
        try:
            silver_table_path = f"{self.silver_path}/{table_name}"
            
            # Vacuum the table
            self.spark.sql(f"VACUUM '{silver_table_path}' RETAIN {retention_hours} HOURS")
            
            logger.info(f"Successfully vacuumed silver table: {table_name}")
            
        except Exception as e:
            logger.error(f"Error vacuuming silver table: {str(e)}")
            raise 