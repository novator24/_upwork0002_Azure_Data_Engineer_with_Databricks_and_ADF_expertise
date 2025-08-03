"""
Generate a visual diagram of the Databricks Data Engineering Demo project architecture.
"""

import matplotlib.pyplot as plt
import matplotlib.patches as patches
from matplotlib.patches import FancyBboxPatch, ConnectionPatch
import numpy as np

# Set up the figure
fig, ax = plt.subplots(1, 1, figsize=(16, 12))
ax.set_xlim(0, 16)
ax.set_ylim(0, 12)
ax.axis('off')

# Define colors
colors = {
    'bronze': '#CD7F32',
    'silver': '#C0C0C0', 
    'gold': '#FFD700',
    'cloud': '#87CEEB',
    'databricks': '#FF6B35',
    'quality': '#90EE90',
    'ci_cd': '#FF69B4'
}

# Data Sources Layer
ax.text(2, 11, 'Data Sources', fontsize=14, fontweight='bold', ha='center')
ax.text(2, 10.5, 'AWS S3', fontsize=10, ha='center')
ax.text(2, 10.2, 'Azure Data Lake', fontsize=10, ha='center')
ax.text(2, 9.9, 'APIs', fontsize=10, ha='center')

# Bronze Layer
bronze_box = FancyBboxPatch((4, 9), 2, 2, boxstyle="round,pad=0.1", 
                            facecolor=colors['bronze'], edgecolor='black', linewidth=2)
ax.add_patch(bronze_box)
ax.text(5, 10.5, 'BRONZE', fontsize=12, fontweight='bold', ha='center')
ax.text(5, 10.2, 'Raw Data', fontsize=9, ha='center')
ax.text(5, 9.9, 'Ingestion', fontsize=9, ha='center')
ax.text(5, 9.6, 'Validation', fontsize=9, ha='center')

# Silver Layer
silver_box = FancyBboxPatch((7, 9), 2, 2, boxstyle="round,pad=0.1", 
                            facecolor=colors['silver'], edgecolor='black', linewidth=2)
ax.add_patch(silver_box)
ax.text(8, 10.5, 'SILVER', fontsize=12, fontweight='bold', ha='center')
ax.text(8, 10.2, 'Cleaned Data', fontsize=9, ha='center')
ax.text(8, 9.9, 'Transformation', fontsize=9, ha='center')
ax.text(8, 9.6, 'Business Rules', fontsize=9, ha='center')

# Gold Layer
gold_box = FancyBboxPatch((10, 9), 2, 2, boxstyle="round,pad=0.1", 
                          facecolor=colors['gold'], edgecolor='black', linewidth=2)
ax.add_patch(gold_box)
ax.text(11, 10.5, 'GOLD', fontsize=12, fontweight='bold', ha='center')
ax.text(11, 10.2, 'Aggregated Data', fontsize=9, ha='center')
ax.text(11, 9.9, 'Business Intelligence', fontsize=9, ha='center')
ax.text(11, 9.6, 'Reporting', fontsize=9, ha='center')

# Databricks Platform
databricks_box = FancyBboxPatch((6, 6), 4, 2, boxstyle="round,pad=0.1", 
                                facecolor=colors['databricks'], edgecolor='black', linewidth=2)
ax.add_patch(databricks_box)
ax.text(8, 7.5, 'DATABRICKS', fontsize=14, fontweight='bold', ha='center')
ax.text(8, 7.2, 'Unified Analytics Platform', fontsize=10, ha='center')
ax.text(8, 6.9, 'Apache Spark + Delta Lake', fontsize=10, ha='center')
ax.text(8, 6.6, 'MLflow + dbt Integration', fontsize=10, ha='center')

# Data Quality
quality_box = FancyBboxPatch((11, 6), 3, 2, boxstyle="round,pad=0.1", 
                             facecolor=colors['quality'], edgecolor='black', linewidth=2)
ax.add_patch(quality_box)
ax.text(12.5, 7.5, 'DATA QUALITY', fontsize=12, fontweight='bold', ha='center')
ax.text(12.5, 7.2, 'Validation', fontsize=9, ha='center')
ax.text(12.5, 6.9, 'Monitoring', fontsize=9, ha='center')
ax.text(12.5, 6.6, 'Alerting', fontsize=9, ha='center')

# Cloud Storage
storage_box = FancyBboxPatch((1, 6), 3, 2, boxstyle="round,pad=0.1", 
                             facecolor=colors['cloud'], edgecolor='black', linewidth=2)
ax.add_patch(storage_box)
ax.text(2.5, 7.5, 'CLOUD STORAGE', fontsize=12, fontweight='bold', ha='center')
ax.text(2.5, 7.2, 'AWS S3', fontsize=9, ha='center')
ax.text(2.5, 6.9, 'Azure Data Lake', fontsize=9, ha='center')
ax.text(2.5, 6.6, 'Delta Lake Format', fontsize=9, ha='center')

# CI/CD Pipeline
cicd_box = FancyBboxPatch((6, 3), 4, 2, boxstyle="round,pad=0.1", 
                          facecolor=colors['ci_cd'], edgecolor='black', linewidth=2)
ax.add_patch(cicd_box)
ax.text(8, 4.5, 'CI/CD PIPELINE', fontsize=12, fontweight='bold', ha='center')
ax.text(8, 4.2, 'GitHub Actions', fontsize=9, ha='center')
ax.text(8, 3.9, 'Terraform', fontsize=9, ha='center')
ax.text(8, 3.6, 'Automated Testing', fontsize=9, ha='center')

# Infrastructure
infra_box = FancyBboxPatch((11, 3), 3, 2, boxstyle="round,pad=0.1", 
                           facecolor='lightgray', edgecolor='black', linewidth=2)
ax.add_patch(infra_box)
ax.text(12.5, 4.5, 'INFRASTRUCTURE', fontsize=12, fontweight='bold', ha='center')
ax.text(12.5, 4.2, 'Azure Databricks', fontsize=9, ha='center')
ax.text(12.5, 3.9, 'Auto-scaling Clusters', fontsize=9, ha='center')
ax.text(12.5, 3.6, 'Cost Optimization', fontsize=9, ha='center')

# Arrows showing data flow
# Data Sources to Bronze
arrow1 = ConnectionPatch((2.5, 9.5), (4, 10), "data", "data", 
                        arrowstyle="->", shrinkA=5, shrinkB=5, mutation_scale=20, fc="black")
ax.add_patch(arrow1)

# Bronze to Silver
arrow2 = ConnectionPatch((6, 10), (7, 10), "data", "data", 
                        arrowstyle="->", shrinkA=5, shrinkB=5, mutation_scale=20, fc="black")
ax.add_patch(arrow2)

# Silver to Gold
arrow3 = ConnectionPatch((9, 10), (10, 10), "data", "data", 
                        arrowstyle="->", shrinkA=5, shrinkB=5, mutation_scale=20, fc="black")
ax.add_patch(arrow3)

# Storage connections
arrow4 = ConnectionPatch((2.5, 7), (4, 9), "data", "data", 
                        arrowstyle="->", shrinkA=5, shrinkB=5, mutation_scale=20, fc="blue", alpha=0.7)
ax.add_patch(arrow4)

arrow5 = ConnectionPatch((2.5, 6.5), (7, 9), "data", "data", 
                        arrowstyle="->", shrinkA=5, shrinkB=5, mutation_scale=20, fc="blue", alpha=0.7)
ax.add_patch(arrow5)

arrow6 = ConnectionPatch((2.5, 6), (10, 9), "data", "data", 
                        arrowstyle="->", shrinkA=5, shrinkB=5, mutation_scale=20, fc="blue", alpha=0.7)
ax.add_patch(arrow6)

# Quality monitoring arrows
arrow7 = ConnectionPatch((8, 9), (11, 7.5), "data", "data", 
                        arrowstyle="->", shrinkA=5, shrinkB=5, mutation_scale=20, fc="green", alpha=0.7)
ax.add_patch(arrow7)

arrow8 = ConnectionPatch((8, 9), (12.5, 7.5), "data", "data", 
                        arrowstyle="->", shrinkA=5, shrinkB=5, mutation_scale=20, fc="green", alpha=0.7)
ax.add_patch(arrow8)

# CI/CD to infrastructure
arrow9 = ConnectionPatch((8, 3), (11, 4.5), "data", "data", 
                        arrowstyle="->", shrinkA=5, shrinkB=5, mutation_scale=20, fc="red", alpha=0.7)
ax.add_patch(arrow9)

# Add title
ax.text(8, 11.5, 'Databricks Data Engineering Demo Project', fontsize=18, fontweight='bold', ha='center')

# Add legend
legend_elements = [
    patches.Patch(color=colors['bronze'], label='Bronze Layer (Raw Data)'),
    patches.Patch(color=colors['silver'], label='Silver Layer (Cleaned Data)'),
    patches.Patch(color=colors['gold'], label='Gold Layer (Business Intelligence)'),
    patches.Patch(color=colors['databricks'], label='Databricks Platform'),
    patches.Patch(color=colors['quality'], label='Data Quality & Monitoring'),
    patches.Patch(color=colors['ci_cd'], label='CI/CD Pipeline'),
    patches.Patch(color=colors['cloud'], label='Cloud Storage')
]

ax.legend(handles=legend_elements, loc='upper left', bbox_to_anchor=(0, 0.3))

# Save the diagram
plt.tight_layout()
plt.savefig('project_architecture.png', dpi=300, bbox_inches='tight', facecolor='white')
plt.close()

print("Project architecture diagram saved as 'project_architecture.png'") 