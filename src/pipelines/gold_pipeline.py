"""
Gold Layer Pipeline - Business Intelligence and Aggregated Data

This module handles the creation of business-ready datasets for analytics and reporting.
"""

from typing import Dict, Any
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import sum, count, avg, current_timestamp, lit
from src.utils.config import Config
from src.utils.logger import get_logger

logger = get_logger(__name__)


class GoldPipeline:
    """Gold Layer Pipeline for business intelligence and aggregated data."""
    
    def __init__(self, spark: SparkSession, config: Config):
        self.spark = spark
        self.config = config
        self.silver_path = config.get("silver_layer_path", "s3://data-lake/silver")
        self.gold_path = config.get("gold_layer_path", "s3://data-lake/gold")
    
    def create_customer_summary(self, customer_table: str, sales_table: str) -> DataFrame:
        """Create customer summary with sales metrics."""
        try:
            logger.info("Creating customer summary from silver layer")
            
            # Read silver tables
            customers_df = self.spark.read.format("delta").load(f"{self.silver_path}/{customer_table}")
            sales_df = self.spark.read.format("delta").load(f"{self.silver_path}/{sales_table}")
            
            # Join and aggregate
            customer_summary = customers_df.join(sales_df, "customer_id", "left") \
                .groupBy("customer_id", "first_name", "last_name", "email", "city", "state") \
                .agg(
                    count("order_id").alias("total_orders"),
                    sum("total_amount").alias("total_spent"),
                    avg("total_amount").alias("avg_order_value")
                )
            
            # Write to gold layer
            gold_table_path = f"{self.gold_path}/customer_summary"
            customer_summary.write \
                .format("delta") \
                .mode("overwrite") \
                .save(gold_table_path)
            
            logger.info(f"Created customer summary with {customer_summary.count()} records")
            return customer_summary
            
        except Exception as e:
            logger.error(f"Error creating customer summary: {str(e)}")
            raise
    
    def create_sales_summary(self, sales_table: str) -> DataFrame:
        """Create sales summary by product and category."""
        try:
            logger.info("Creating sales summary from silver layer")
            
            # Read sales data
            sales_df = self.spark.read.format("delta").load(f"{self.silver_path}/{sales_table}")
            
            # Create product summary
            product_summary = sales_df.groupBy("product_name", "product_category") \
                .agg(
                    count("order_id").alias("total_orders"),
                    sum("quantity").alias("total_quantity"),
                    sum("total_amount").alias("total_revenue"),
                    avg("unit_price").alias("avg_unit_price")
                )
            
            # Write to gold layer
            gold_table_path = f"{self.gold_path}/product_summary"
            product_summary.write \
                .format("delta") \
                .mode("overwrite") \
                .save(gold_table_path)
            
            logger.info(f"Created product summary with {product_summary.count()} records")
            return product_summary
            
        except Exception as e:
            logger.error(f"Error creating sales summary: {str(e)}")
            raise 