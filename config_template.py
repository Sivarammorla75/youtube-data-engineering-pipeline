# Configuration file for YouTube Data Engineering Pipeline
# Copy this file to config.py and fill in your actual credentials

# YouTube API Configuration
YOUTUBE_API_KEY = "YOUR_YOUTUBE_API_KEY_HERE"

# MySQL Database Configuration
DB_CONFIG = {
    "host": "localhost",
    "user": "root",
    "password": "YOUR_MYSQL_PASSWORD_HERE",
    "database": "youtube_pipeline"
}

# Pipeline Configuration
MAX_RESULTS = 50
REGION_CODE = "US"
CHART_TYPE = "mostPopular"

# Logging Configuration
LOG_LEVEL = "INFO"
LOG_FILE = "logs/pipeline.log"