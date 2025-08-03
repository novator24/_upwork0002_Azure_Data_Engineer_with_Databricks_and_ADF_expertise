"""
Unit tests for Bronze Pipeline
"""

import pytest
from unittest.mock import Mock, patch
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, TimestampType

from src.pipelines.bronze_pipeline import BronzePipeline
from src.utils.config import Config


class TestBronzePipeline:
    """Test cases for BronzePipeline class."""
    
    @pytest.fixture
    def mock_spark(self):
        """Create a mock Spark session."""
        spark = Mock(spec=SparkSession)
        spark.read.format.return_value.load.return_value = Mock()
        return spark
    
    @pytest.fixture
    def mock_config(self):
        """Create a mock configuration."""
        config = Mock(spec=Config)
        config.get.return_value = "s3://data-lake/bronze"
        return config
    
    @pytest.fixture
    def bronze_pipeline(self, mock_spark, mock_config):
        """Create a BronzePipeline instance with mocked dependencies."""
        return BronzePipeline(mock_spark, mock_config)
    
    def test_init(self, mock_spark, mock_config):
        """Test BronzePipeline initialization."""
        pipeline = BronzePipeline(mock_spark, mock_config)
        
        assert pipeline.spark == mock_spark
        assert pipeline.config == mock_config
        assert pipeline.bronze_path == "s3://data-lake/bronze"
    
    def test_ingest_from_s3_success(self, bronze_pipeline, mock_spark):
        """Test successful S3 data ingestion."""
        # Mock DataFrame
        mock_df = Mock()
        mock_df.count.return_value = 100
        mock_df.withColumn.return_value = mock_df
        
        # Mock Spark read
        mock_spark.read.format.return_value.load.return_value = mock_df
        
        # Mock write operation
        mock_write = Mock()
        mock_df.write = mock_write
        mock_write.format.return_value.mode.return_value.option.return_value.option.return_value.save.return_value = None
        
        result = bronze_pipeline.ingest_from_s3("test-bucket", "test-prefix", "test-table")
        
        assert result == mock_df
        mock_df.count.assert_called_once()
    
    def test_ingest_from_s3_failure(self, bronze_pipeline, mock_spark):
        """Test S3 data ingestion failure."""
        # Mock Spark read to raise exception
        mock_spark.read.format.return_value.load.side_effect = Exception("S3 connection failed")
        
        with pytest.raises(Exception, match="S3 connection failed"):
            bronze_pipeline.ingest_from_s3("test-bucket", "test-prefix", "test-table")
    
    def test_ingest_from_azure_datalake_success(self, bronze_pipeline, mock_spark):
        """Test successful Azure Data Lake ingestion."""
        # Mock DataFrame
        mock_df = Mock()
        mock_df.count.return_value = 150
        mock_df.withColumn.return_value = mock_df
        
        # Mock Spark read
        mock_spark.read.format.return_value.load.return_value = mock_df
        
        # Mock write operation
        mock_write = Mock()
        mock_df.write = mock_write
        mock_write.format.return_value.mode.return_value.option.return_value.save.return_value = None
        
        result = bronze_pipeline.ingest_from_azure_datalake("test-container", "test-path", "test-table")
        
        assert result == mock_df
        mock_df.count.assert_called_once()
    
    def test_add_ingestion_metadata(self, bronze_pipeline):
        """Test metadata addition to DataFrame."""
        # Mock DataFrame
        mock_df = Mock()
        mock_df.withColumn.return_value = mock_df
        
        result = bronze_pipeline._add_ingestion_metadata(mock_df, "s3", "s3://bucket/path")
        
        assert result == mock_df
        # Verify withColumn was called for each metadata column
        assert mock_df.withColumn.call_count == 4
    
    def test_validate_bronze_data(self, bronze_pipeline, mock_spark):
        """Test bronze data validation."""
        # Mock DataFrame with columns
        mock_df = Mock()
        mock_df.columns = ["customer_id", "name", "email", "_ingestion_timestamp"]
        mock_df.count.return_value = 100
        mock_df.filter.return_value.count.return_value = 5
        mock_df.groupBy.return_value.count.return_value.filter.return_value.count.return_value = 2
        
        # Mock Spark read
        mock_spark.read.format.return_value.load.return_value = mock_df
        
        result = bronze_pipeline.validate_bronze_data("test-table")
        
        assert result["table_name"] == "test-table"
        assert result["total_records"] == 100
        assert result["duplicate_records"] == 2
        assert result["schema_validation"] == "PASS"
    
    def test_optimize_bronze_table(self, bronze_pipeline, mock_spark):
        """Test bronze table optimization."""
        # Mock Spark SQL
        mock_spark.sql.return_value = None
        
        bronze_pipeline.optimize_bronze_table("test-table")
        
        # Verify SQL commands were executed
        assert mock_spark.sql.call_count == 3
    
    def test_get_bronze_table_info(self, bronze_pipeline, mock_spark):
        """Test getting bronze table information."""
        # Mock DataFrame
        mock_df = Mock()
        mock_df.columns = ["col1", "col2"]
        mock_df.count.return_value = 100
        mock_df.schema.json.return_value = '{"type": "struct"}'
        
        # Mock DeltaTable
        mock_delta_table = Mock()
        mock_detail = Mock()
        mock_detail.select.return_value.collect.return_value = [[["col1"], 10, 1000]]
        
        with patch('src.pipelines.bronze_pipeline.DeltaTable') as mock_delta_table_class:
            mock_delta_table_class.forPath.return_value = mock_delta_table
            mock_delta_table.detail.return_value = mock_detail
            
            # Mock Spark read
            mock_spark.read.format.return_value.load.return_value = mock_df
            
            result = bronze_pipeline.get_bronze_table_info("test-table")
            
            assert result["table_name"] == "test-table"
            assert result["total_records"] == 100
            assert "schema" in result 