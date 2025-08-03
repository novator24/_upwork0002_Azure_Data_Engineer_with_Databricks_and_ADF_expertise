#!/usr/bin/env python3
"""
Generate PNG from HTML diagram using a simple approach.
This script creates a simple PNG representation of the project architecture.
"""

import base64
from PIL import Image, ImageDraw, ImageFont
import os

def create_architecture_diagram():
    """Create a simple PNG diagram of the project architecture."""
    
    # Create a new image with white background
    width, height = 1200, 800
    image = Image.new('RGB', (width, height), 'white')
    draw = ImageDraw.Draw(image)
    
    # Try to use a default font, fallback to basic if not available
    try:
        font_large = ImageFont.truetype("/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf", 24)
        font_medium = ImageFont.truetype("/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf", 16)
        font_small = ImageFont.truetype("/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf", 12)
    except:
        # Fallback to default font
        font_large = ImageFont.load_default()
        font_medium = ImageFont.load_default()
        font_small = ImageFont.load_default()
    
    # Colors
    colors = {
        'bronze': '#CD7F32',
        'silver': '#C0C0C0',
        'gold': '#FFD700',
        'databricks': '#FF6B35',
        'blue': '#87CEEB',
        'green': '#90EE90',
        'purple': '#9370DB'
    }
    
    # Title
    draw.text((width//2, 30), "Databricks Data Engineering Demo Project", 
              fill='black', font=font_large, anchor='mm')
    
    # Data Sources
    draw.rectangle([50, 80, 350, 150], fill=colors['blue'], outline='black', width=2)
    draw.text((200, 115), "DATA SOURCES", fill='white', font=font_medium, anchor='mm')
    draw.text((200, 135), "AWS S3 • Azure Data Lake • APIs", fill='white', font=font_small, anchor='mm')
    
    # Arrow
    draw.text((200, 170), "↓", fill='black', font=font_large, anchor='mm')
    
    # Bronze Layer
    draw.rectangle([50, 190, 350, 260], fill=colors['bronze'], outline='black', width=2)
    draw.text((200, 225), "BRONZE LAYER", fill='white', font=font_medium, anchor='mm')
    draw.text((200, 245), "Raw Data • Validation • Metadata", fill='white', font=font_small, anchor='mm')
    
    # Arrow
    draw.text((200, 280), "↓", fill='black', font=font_large, anchor='mm')
    
    # Silver Layer
    draw.rectangle([50, 300, 350, 370], fill=colors['silver'], outline='black', width=2)
    draw.text((200, 335), "SILVER LAYER", fill='black', font=font_medium, anchor='mm')
    draw.text((200, 355), "Cleaned Data • Business Rules", fill='black', font=font_small, anchor='mm')
    
    # Arrow
    draw.text((200, 390), "↓", fill='black', font=font_large, anchor='mm')
    
    # Gold Layer
    draw.rectangle([50, 410, 350, 480], fill=colors['gold'], outline='black', width=2)
    draw.text((200, 445), "GOLD LAYER", fill='black', font=font_medium, anchor='mm')
    draw.text((200, 465), "Business Intelligence • Reporting", fill='black', font=font_small, anchor='mm')
    
    # Databricks Platform
    draw.rectangle([400, 190, 700, 260], fill=colors['databricks'], outline='black', width=2)
    draw.text((550, 225), "DATABRICKS PLATFORM", fill='white', font=font_medium, anchor='mm')
    draw.text((550, 245), "Apache Spark • Delta Lake • MLflow", fill='white', font=font_small, anchor='mm')
    
    # Data Quality
    draw.rectangle([400, 300, 700, 370], fill=colors['green'], outline='black', width=2)
    draw.text((550, 335), "DATA QUALITY", fill='black', font=font_medium, anchor='mm')
    draw.text((550, 355), "Validation • Monitoring • Alerting", fill='black', font=font_small, anchor='mm')
    
    # CI/CD Pipeline
    draw.rectangle([400, 410, 700, 480], fill=colors['purple'], outline='black', width=2)
    draw.text((550, 445), "CI/CD PIPELINE", fill='white', font=font_medium, anchor='mm')
    draw.text((550, 465), "GitHub Actions • Terraform • Testing", fill='white', font=font_small, anchor='mm')
    
    # Technologies
    draw.rectangle([50, 520, 1150, 650], fill='lightgray', outline='black', width=2)
    draw.text((600, 540), "TECHNOLOGIES", fill='black', font=font_medium, anchor='mm')
    
    tech_list = [
        "Databricks", "Apache Spark", "Delta Lake", "Python/Scala",
        "AWS S3", "Azure Data Lake", "GitHub Actions", "Terraform",
        "MLflow", "dbt", "Apache Airflow", "Great Expectations"
    ]
    
    x_start = 80
    y_start = 570
    x_offset = 0
    y_offset = 0
    
    for i, tech in enumerate(tech_list):
        if i % 4 == 0 and i > 0:
            x_offset = 0
            y_offset += 30
        draw.text((x_start + x_offset * 250, y_start + y_offset), 
                  f"• {tech}", fill='black', font=font_small)
        x_offset += 1
    
    # Key Features
    draw.rectangle([50, 680, 1150, 750], fill='lightblue', outline='black', width=2)
    draw.text((600, 700), "KEY FEATURES", fill='black', font=font_medium, anchor='mm')
    draw.text((600, 720), "ETL/ELT Pipelines • Delta Lake ACID • Multi-cloud • Data Quality • Performance Optimization • CI/CD", 
              fill='black', font=font_small, anchor='mm')
    
    # Save the image
    image.save('project_architecture.png', 'PNG')
    print("Project architecture diagram saved as 'project_architecture.png'")

if __name__ == "__main__":
    create_architecture_diagram() 