"""
Data quality validation utilities.
"""

from typing import Dict, Any, List
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, count, isnan, isnull
from src.utils.config import Config
from src.utils.logger import get_logger

logger = get_logger(__name__)


class DataQualityChecker:
    """Data quality validation and monitoring."""
    
    def __init__(self, spark: SparkSession, config: Config):
        self.spark = spark
        self.config = config
        self.null_threshold = config.get("data_quality.null_threshold", 0.1)
        self.duplicate_threshold = config.get("data_quality.duplicate_threshold", 0.05)
    
    def validate_customer_data(self, df: DataFrame) -> Dict[str, Any]:
        """Validate customer data quality."""
        total_records = df.count()
        
        # Check null values
        null_counts = {}
        for column in df.columns:
            if not column.startswith("_"):
                null_count = df.filter(col(column).isNull()).count()
                null_counts[column] = null_count
        
        # Check duplicates
        data_columns = [col for col in df.columns if not col.startswith("_")]
        duplicate_count = 0
        if data_columns:
            duplicate_count = df.groupBy(data_columns).count().filter("count > 1").count()
        
        return {
            "total_records": total_records,
            "null_counts": null_counts,
            "duplicate_records": duplicate_count,
            "quality_score": self._calculate_quality_score(total_records, null_counts, duplicate_count)
        }
    
    def validate_sales_data(self, df: DataFrame) -> Dict[str, Any]:
        """Validate sales data quality."""
        total_records = df.count()
        
        # Check for negative amounts
        negative_amounts = df.filter(col("total_amount") < 0).count()
        
        # Check for zero quantities
        zero_quantities = df.filter(col("quantity") <= 0).count()
        
        return {
            "total_records": total_records,
            "negative_amounts": negative_amounts,
            "zero_quantities": zero_quantities,
            "quality_score": self._calculate_sales_quality_score(total_records, negative_amounts, zero_quantities)
        }
    
    def _calculate_quality_score(self, total: int, null_counts: Dict, duplicates: int) -> float:
        """Calculate overall data quality score."""
        if total == 0:
            return 0.0
        
        null_penalty = sum(null_counts.values()) / total
        duplicate_penalty = duplicates / total
        
        quality_score = 1.0 - (null_penalty + duplicate_penalty)
        return max(0.0, quality_score)
    
    def _calculate_sales_quality_score(self, total: int, negative_amounts: int, zero_quantities: int) -> float:
        """Calculate sales data quality score."""
        if total == 0:
            return 0.0
        
        penalty = (negative_amounts + zero_quantities) / total
        quality_score = 1.0 - penalty
        return max(0.0, quality_score) 