# Deployment Guide

This guide provides step-by-step instructions for deploying the Databricks Data Engineering Demo project.

## Prerequisites

### Required Tools
- **Terraform** (v1.0+)
- **Python** (3.8+)
- **Azure CLI** (latest)
- **Git** (latest)

### Required Permissions
- Azure Subscription Owner/Contributor
- Databricks Workspace Admin
- Storage Account Contributor

## Environment Setup

### 1. Clone the Repository
```bash
git clone <repository-url>
cd databricks-data-engineering-demo
```

### 2. Install Dependencies
```bash
# Create virtual environment
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt
pip install -r requirements-dev.txt
```

### 3. Configure Environment Variables
Create a `.env` file in the project root:
```bash
# Azure Configuration
AZURE_SUBSCRIPTION_ID=your-subscription-id
AZURE_TENANT_ID=your-tenant-id
AZURE_CLIENT_ID=your-client-id
AZURE_CLIENT_SECRET=your-client-secret

# Databricks Configuration
DATABRICKS_HOST=https://your-workspace.azuredatabricks.net
DATABRICKS_TOKEN=your-databricks-token

# AWS Configuration (if using S3)
AWS_ACCESS_KEY_ID=your-access-key
AWS_SECRET_ACCESS_KEY=your-secret-key
AWS_DEFAULT_REGION=us-east-1

# Storage Configuration
AZURE_STORAGE_CONNECTION_STRING=your-storage-connection-string
```

## Infrastructure Deployment

### 1. Azure Authentication
```bash
# Login to Azure
az login

# Set subscription
az account set --subscription $AZURE_SUBSCRIPTION_ID
```

### 2. Deploy Infrastructure with Terraform
```bash
# Navigate to Terraform directory
cd infrastructure/terraform

# Initialize Terraform
terraform init

# Plan the deployment
terraform plan -out=tfplan

# Apply the configuration
terraform apply tfplan
```

### 3. Verify Infrastructure Deployment
```bash
# Check resource group
az group show --name databricks-data-engineering-demo

# Check Databricks workspace
az databricks workspace show --name databricks-data-engineering-demo --resource-group databricks-data-engineering-demo

# Check storage account
az storage account show --name databricksdatalake<random-suffix> --resource-group databricks-data-engineering-demo
```

## Databricks Workspace Configuration

### 1. Access Databricks Workspace
- Navigate to the Databricks workspace URL from Terraform output
- Sign in with your Azure credentials

### 2. Configure Cluster
1. Go to **Clusters** in the Databricks workspace
2. Create a new cluster with the following settings:
   - **Cluster Name**: `data-processing-cluster`
   - **Spark Version**: `11.3.x-scala2.12`
   - **Node Type**: `Standard_DS3_v2`
   - **Min Workers**: `1`
   - **Max Workers**: `3`
   - **Auto Termination**: `20 minutes`

3. Add the following Spark configurations:
   ```
   spark.sql.adaptive.enabled=true
   spark.sql.adaptive.coalescePartitions.enabled=true
   spark.sql.adaptive.skewJoin.enabled=true
   spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension
   spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog
   ```

### 3. Install Libraries
1. Go to **Libraries** in the Databricks workspace
2. Install the following libraries:
   - `delta-spark==2.4.0`
   - `boto3==1.28.0`
   - `azure-storage-blob==12.17.0`
   - `great-expectations==0.17.0`

### 4. Configure Secrets
1. Go to **Secrets** in the Databricks workspace
2. Create the following secrets:
   - `aws-access-key-id`
   - `aws-secret-access-key`
   - `azure-storage-connection-string`

## Pipeline Deployment

### 1. Upload Code to Databricks
```bash
# Install Databricks CLI
pip install databricks-cli

# Configure Databricks CLI
databricks configure --token

# Upload notebooks
databricks fs cp -r notebooks/ dbfs:/Repos/Data\ Engineering\ Demo/notebooks/

# Upload source code
databricks fs cp -r src/ dbfs:/Repos/Data\ Engineering\ Demo/src/
```

### 2. Deploy Pipeline Code
```bash
# Run the deployment script
python -m src.pipelines.deploy --config config/pipeline_config.json
```

### 3. Create Databricks Jobs
1. Go to **Jobs** in the Databricks workspace
2. Create the following jobs:

#### Bronze Layer Job
- **Job Name**: `Bronze Layer Pipeline`
- **Notebook Path**: `/Repos/Data Engineering Demo/notebooks/bronze/01_data_ingestion`
- **Cluster**: `data-processing-cluster`
- **Schedule**: `0 0 1 * * ?` (Daily at 1 AM UTC)

#### Silver Layer Job
- **Job Name**: `Silver Layer Pipeline`
- **Notebook Path**: `/Repos/Data Engineering Demo/notebooks/silver/01_data_processing`
- **Cluster**: `data-processing-cluster`
- **Schedule**: `0 30 1 * * ?` (Daily at 1:30 AM UTC)

#### Gold Layer Job
- **Job Name**: `Gold Layer Pipeline`
- **Notebook Path**: `/Repos/Data Engineering Demo/notebooks/gold/01_data_aggregation`
- **Cluster**: `data-processing-cluster`
- **Schedule**: `0 0 2 * * ?` (Daily at 2 AM UTC)

#### Data Quality Job
- **Job Name**: `Data Quality Pipeline`
- **Notebook Path**: `/Repos/Data Engineering Demo/notebooks/quality/01_data_quality_checks`
- **Cluster**: `data-processing-cluster`
- **Schedule**: `0 15 2 * * ?` (Daily at 2:15 AM UTC)

## Data Source Configuration

### 1. AWS S3 Configuration
1. Create an S3 bucket for data lake storage
2. Configure bucket permissions for Databricks access
3. Update the configuration file with bucket details

### 2. Azure Data Lake Configuration
1. Create storage containers for each layer
2. Configure access policies
3. Update connection strings in configuration

### 3. Sample Data Generation
```bash
# Generate sample data for testing
python scripts/generate_sample_data.py

# Upload sample data to storage
python scripts/upload_sample_data.py
```

## Testing and Validation

### 1. Run Unit Tests
```bash
# Run unit tests
pytest tests/unit/ -v

# Run with coverage
pytest tests/unit/ -v --cov=src --cov-report=html
```

### 2. Run Integration Tests
```bash
# Run integration tests
pytest tests/integration/ -v
```

### 3. Test Pipeline End-to-End
```bash
# Test complete pipeline
python -m src.pipelines.deploy --config config/pipeline_config.json --layers bronze silver gold quality
```

### 4. Validate Data Quality
```bash
# Run data quality checks
python -m tests.data_quality
```

## Monitoring and Alerting

### 1. Configure Monitoring
1. Set up Databricks monitoring
2. Configure alerting rules
3. Set up dashboard for pipeline monitoring

### 2. Set up Alerts
- Pipeline failure notifications
- Data quality violation alerts
- Performance degradation alerts

### 3. Create Dashboards
- Pipeline execution status
- Data quality metrics
- Performance metrics
- Cost monitoring

## Troubleshooting

### Common Issues

#### 1. Authentication Issues
```bash
# Verify Azure credentials
az account show

# Verify Databricks token
databricks clusters list
```

#### 2. Cluster Issues
- Check cluster logs in Databricks
- Verify Spark configurations
- Check resource allocation

#### 3. Data Quality Issues
- Review data quality logs
- Check data source connectivity
- Verify schema configurations

#### 4. Performance Issues
- Monitor cluster utilization
- Check query performance
- Optimize Spark configurations

### Debug Commands
```bash
# Check pipeline status
python -m src.pipelines.deploy --config config/pipeline_config.json --layers bronze

# Validate configuration
python -c "from src.utils.config import Config; Config('config/pipeline_config.json')"

# Test data connectivity
python scripts/test_connectivity.py
```

## Security Considerations

### 1. Access Control
- Implement role-based access control
- Use Azure AD integration
- Configure network security

### 2. Data Encryption
- Enable encryption at rest
- Enable encryption in transit
- Use customer-managed keys

### 3. Audit Logging
- Enable audit logging
- Monitor access patterns
- Review security logs

## Cost Optimization

### 1. Resource Optimization
- Right-size clusters
- Use auto-scaling
- Implement auto-termination

### 2. Storage Optimization
- Use appropriate storage tiers
- Implement lifecycle policies
- Optimize data formats

### 3. Monitoring Costs
- Set up cost alerts
- Monitor resource usage
- Optimize based on usage patterns

## Maintenance

### 1. Regular Maintenance
- Update dependencies
- Review and optimize configurations
- Monitor performance metrics

### 2. Backup and Recovery
- Backup configurations
- Test recovery procedures
- Document disaster recovery plan

### 3. Updates and Upgrades
- Plan for Spark version upgrades
- Test compatibility
- Implement gradual rollouts

## Support and Documentation

### 1. Documentation
- Keep documentation updated
- Document configuration changes
- Maintain runbooks

### 2. Training
- Train team members
- Document procedures
- Create knowledge base

### 3. Support Process
- Define escalation procedures
- Set up support channels
- Document troubleshooting guides 