"""
Data Quality Pipeline - Monitoring and Validation

This module handles automated data quality checks and monitoring across all layers.
"""

from typing import Dict, Any, List
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, count, current_timestamp
from src.utils.config import Config
from src.utils.logger import get_logger
from src.utils.data_quality import DataQualityChecker

logger = get_logger(__name__)


class DataQualityPipeline:
    """Data Quality Pipeline for monitoring and validation."""
    
    def __init__(self, spark: SparkSession, config: Config):
        self.spark = spark
        self.config = config
        self.data_quality_checker = DataQualityChecker(spark, config)
        self.bronze_path = config.get("bronze_layer_path", "s3://data-lake/bronze")
        self.silver_path = config.get("silver_layer_path", "s3://data-lake/silver")
        self.gold_path = config.get("gold_layer_path", "s3://data-lake/gold")
    
    def run_bronze_quality_checks(self, table_name: str) -> Dict[str, Any]:
        """Run quality checks on bronze layer data."""
        try:
            logger.info(f"Running quality checks on bronze table: {table_name}")
            
            bronze_table_path = f"{self.bronze_path}/{table_name}"
            df = self.spark.read.format("delta").load(bronze_table_path)
            
            quality_results = self.data_quality_checker.validate_customer_data(df)
            quality_results["layer"] = "bronze"
            quality_results["table"] = table_name
            
            # Log results
            self._log_quality_results(quality_results)
            
            return quality_results
            
        except Exception as e:
            logger.error(f"Error running bronze quality checks: {str(e)}")
            raise
    
    def run_silver_quality_checks(self, table_name: str) -> Dict[str, Any]:
        """Run quality checks on silver layer data."""
        try:
            logger.info(f"Running quality checks on silver table: {table_name}")
            
            silver_table_path = f"{self.silver_path}/{table_name}"
            df = self.spark.read.format("delta").load(silver_table_path)
            
            if "sales" in table_name.lower():
                quality_results = self.data_quality_checker.validate_sales_data(df)
            else:
                quality_results = self.data_quality_checker.validate_customer_data(df)
            
            quality_results["layer"] = "silver"
            quality_results["table"] = table_name
            
            # Log results
            self._log_quality_results(quality_results)
            
            return quality_results
            
        except Exception as e:
            logger.error(f"Error running silver quality checks: {str(e)}")
            raise
    
    def run_gold_quality_checks(self, table_name: str) -> Dict[str, Any]:
        """Run quality checks on gold layer data."""
        try:
            logger.info(f"Running quality checks on gold table: {table_name}")
            
            gold_table_path = f"{self.gold_path}/{table_name}"
            df = self.spark.read.format("delta").load(gold_table_path)
            
            # Basic quality checks for gold layer
            total_records = df.count()
            null_counts = {}
            
            for column in df.columns:
                null_count = df.filter(col(column).isNull()).count()
                null_counts[column] = null_count
            
            quality_results = {
                "layer": "gold",
                "table": table_name,
                "total_records": total_records,
                "null_counts": null_counts,
                "quality_score": 1.0 - (sum(null_counts.values()) / total_records if total_records > 0 else 0)
            }
            
            # Log results
            self._log_quality_results(quality_results)
            
            return quality_results
            
        except Exception as e:
            logger.error(f"Error running gold quality checks: {str(e)}")
            raise
    
    def _log_quality_results(self, results: Dict[str, Any]):
        """Log quality check results."""
        logger.info(f"Quality check results for {results['layer']}.{results['table']}:")
        logger.info(f"  Total records: {results['total_records']}")
        logger.info(f"  Quality score: {results.get('quality_score', 'N/A'):.2f}")
        
        if 'null_counts' in results:
            for column, null_count in results['null_counts'].items():
                if null_count > 0:
                    logger.warning(f"  Column '{column}' has {null_count} null values")
    
    def generate_quality_report(self, tables: List[str]) -> Dict[str, Any]:
        """Generate comprehensive quality report for multiple tables."""
        try:
            logger.info("Generating comprehensive quality report")
            
            report = {
                "timestamp": current_timestamp(),
                "overall_quality_score": 0.0,
                "layer_reports": {
                    "bronze": [],
                    "silver": [],
                    "gold": []
                }
            }
            
            total_score = 0
            total_tables = 0
            
            for table in tables:
                if "bronze" in table:
                    results = self.run_bronze_quality_checks(table)
                    report["layer_reports"]["bronze"].append(results)
                elif "silver" in table:
                    results = self.run_silver_quality_checks(table)
                    report["layer_reports"]["silver"].append(results)
                elif "gold" in table:
                    results = self.run_gold_quality_checks(table)
                    report["layer_reports"]["gold"].append(results)
                
                if "quality_score" in results:
                    total_score += results["quality_score"]
                    total_tables += 1
            
            if total_tables > 0:
                report["overall_quality_score"] = total_score / total_tables
            
            logger.info(f"Quality report generated with overall score: {report['overall_quality_score']:.2f}")
            return report
            
        except Exception as e:
            logger.error(f"Error generating quality report: {str(e)}")
            raise 