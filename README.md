# YouTube Data Engineering Pipeline

[![Python](https://img.shields.io/badge/Python-3.8+-blue.svg)](https://www.python.org/)
[![MySQL](https://img.shields.io/badge/MySQL-8.0+-orange.svg)](https://www.mysql.com/)
[![YouTube API](https://img.shields.io/badge/YouTube_API-v3-red.svg)](https://developers.google.com/youtube/v3)
[![License](https://img.shields.io/badge/License-MIT-green.svg)](LICENSE)

An end-to-end data engineering pipeline that extracts YouTube data using the YouTube Data API v3, processes it, stores it in MySQL, and generates insightful analytics and visualizations.

## Table of Contents

- [Features](#features)
- [Architecture](#architecture)
- [Prerequisites](#prerequisites)
- [Quick Start](#quick-start)
- [Project Structure](#project-structure)
- [Configuration](#configuration)
- [Pipeline Stages](#pipeline-stages)
- [Sample Output](#sample-output)
- [Data Schema](#data-schema)
- [API Considerations](#api-considerations)
- [Security Considerations](#security-considerations)
- [Deployment & Scheduling](#deployment--scheduling)
- [Monitoring and Logging](#monitoring-and-logging)
- [Troubleshooting](#troubleshooting)
- [Extending the Pipeline](#extending-the-pipeline)
- [Contributing](#contributing)
- [Version](#version)
- [Changelog](#changelog)
- [License](#license)

## Features

- **Extract**: Fetch trending YouTube videos data using YouTube Data API
- **Transform**: Clean data, handle missing values, and add derived metrics
- **Load**: Store processed data in MySQL database with proper schema
- **Analyze**: Generate comprehensive analytics and visualizations
- **Monitor**: Built-in logging and pipeline execution tracking
- **Scalable**: Modular design for easy extension and maintenance

## Architecture

```
YouTube API → Extract → Raw Data → Transform → Clean Data → Load → MySQL → Analyze → Visualizations
```

## Prerequisites

- Python 3.8+
- MySQL 8.0+
- YouTube Data API v3 key (from Google Cloud Console)

## Quick Start

### 1. Clone and Setup

```bash
# Install Python dependencies
pip install -r requirements.txt

# Run setup script
python setup.py
```

### 2. Configuration

Copy the configuration template and add your credentials:

```bash
cp config_template.py config.py
```

Edit `config.py` and add:
- Your YouTube API key
- MySQL database credentials

### 3. Database Setup

```bash
# Create database and tables
mysql -u root -p < database/schema.sql
```

### 4. Run the Pipeline

```bash
# Run the complete pipeline
python run_pipeline.py

# Or run individual stages
python scripts/extract_youtube_data.py
python scripts/transform_data.py
python scripts/load_to_mysql.py
python scripts/analyze_data.py
```

## Project Structure

```
youtube-data-engineering-pipeline/
├── config_template.py          # Configuration template
├── config.py                   # Your configuration (create from template)
├── requirements.txt            # Python dependencies
├── run_pipeline.py            # Main pipeline orchestrator
├── setup.py                   # Setup script
├── README.md                  # This file
├── database/
│   └── schema.sql            # MySQL database schema
├── scripts/
│   ├── extract_youtube_data.py    # Data extraction from YouTube API
│   ├── transform_data.py         # Data cleaning and transformation
│   ├── load_to_mysql.py         # Data loading to MySQL
│   └── analyze_data.py          # Data analysis and visualization
├── data/
│   ├── raw/                    # Raw extracted data
│   ├── processed/             # Cleaned and transformed data
│   └── plots/                 # Generated visualizations
└── logs/                      # Pipeline execution logs
```

## Configuration

### YouTube API Setup

1. Go to [Google Cloud Console](https://console.cloud.google.com/)
2. Create a new project or select existing one
3. Enable YouTube Data API v3
4. Create credentials (API key)
5. Add the API key to `config.py`

### MySQL Setup

1. Install MySQL server
2. Create a database user with appropriate permissions
3. Update database credentials in `config.py`

## Pipeline Stages

### 1. Extract (`extract_youtube_data.py`)
- Fetches trending videos from YouTube API
- Extracts video metadata, statistics, and engagement metrics
- Saves raw data to CSV format

### 2. Transform (`transform_data.py`)
- Cleans and validates data
- Handles missing values and data type conversions
- Adds derived metrics (engagement rate, data quality flags)
- Removes duplicates

### 3. Load (`load_to_mysql.py`)
- Creates database connection
- Inserts cleaned data into MySQL tables
- Includes error handling and logging
- Tracks pipeline execution

### 4. Analyze (`analyze_data.py`)
- Generates comprehensive analytics
- Creates visualizations (charts, histograms, scatter plots)
- Produces summary statistics
- Saves plots and reports

## Data Schema

### youtube_videos table
```sql
CREATE TABLE youtube_videos (
    id INT AUTO_INCREMENT PRIMARY KEY,
    title VARCHAR(255) NOT NULL,
    channel VARCHAR(255) NOT NULL,
    views BIGINT DEFAULT 0,
    likes BIGINT DEFAULT 0,
    engagement_rate DECIMAL(10,6) DEFAULT 0.0,
    extracted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_channel (channel),
    INDEX idx_views (views),
    INDEX idx_extracted_at (extracted_at)
);
```

### pipeline_logs table
```sql
CREATE TABLE pipeline_logs (
    id INT AUTO_INCREMENT PRIMARY KEY,
    stage VARCHAR(50) NOT NULL,
    status VARCHAR(20) NOT NULL,
    message TEXT,
    executed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

## Sample Output

After running the pipeline, you'll get:

### Database Records
```
Total videos in database: 50
Sample videos:
- BossMan Dlow - Motion Party (Official Music Video)... by BossMan Dlow
  Views: 55,294, Likes: 5,555, Engagement: 0.1064
- Malcolm in the Middle: Life's Still Unfair... by Hulu
  Views: 1,612,859, Likes: 30,338, Engagement: 0.0201
```

### Analytics Summary
```
Total Videos Analyzed: 50
Unique Channels: 50
Average Views per Video: 715,847
Maximum Views: 12,322,708
Average Likes per Video: 22,932
Average Engagement Rate: 0.0593
```

### Generated Files
- `data/raw/youtube_raw.csv` - Raw extracted data
- `data/processed/youtube_clean.csv` - Cleaned and transformed data
- `data/plots/top_channels_views.png` - Top channels by views chart
- `data/plots/engagement_distribution.png` - Engagement rate distribution
- `data/plots/views_vs_engagement.png` - Views vs engagement correlation
- `data/analysis_summary.txt` - Detailed statistics report

## API Considerations

### YouTube API Limits
- **Daily quota**: 10,000 units per day (free tier)
- **Per request cost**: 1 unit for search, 1-100 units for video details
- **Rate limits**: 100 requests per 100 seconds per user

### Best Practices
- Implement exponential backoff for API retries
- Cache results to avoid unnecessary API calls
- Monitor API usage in Google Cloud Console
- Consider upgrading to higher quota for production use

## Security Considerations

- Store API keys securely (never commit to version control)
- Use environment variables for sensitive credentials
- Implement proper database access controls
- Regularly rotate API keys and passwords
- Use parameterized queries to prevent SQL injection

## Troubleshooting

### Common Issues

1. **YouTube API quota exceeded**
   - Check your API quota in Google Cloud Console
   - Consider upgrading your API plan

2. **MySQL connection failed**
   - Verify MySQL server is running
   - Check database credentials in `config.py`
   - Ensure user has proper permissions

3. **Missing dependencies**
   - Run `pip install -r requirements.txt`
   - Ensure you're using Python 3.8+

4. **Permission errors**
   - Check file/directory permissions
   - Ensure write access to `data/` and `logs/` directories

### Logs Location

Check `logs/pipeline_*.log` files for detailed error information and execution status.

## Deployment & Scheduling

### Local Development
```bash
# Run once
python run_pipeline.py

# Run individual stages for testing
python scripts/extract_youtube_data.py
```

### Production Scheduling
```bash
# Using cron (Linux/Mac)
0 */4 * * * cd /path/to/pipeline && python run_pipeline.py

# Using Task Scheduler (Windows)
# Create a task to run daily at specific intervals

# Using Apache Airflow
# Import the pipeline as a DAG for orchestration
```

### Docker Deployment
```dockerfile
FROM python:3.9-slim

WORKDIR /app
COPY requirements.txt .
RUN pip install -r requirements.txt

COPY . .
CMD ["python", "run_pipeline.py"]
```

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests if applicable
5. Submit a pull request

## Version

**Current Version:** 1.0.0

## Changelog

### v1.0.0 (2026-03-13)
- Initial release
- Complete ETL pipeline implementation
- YouTube API integration
- MySQL database storage
- Comprehensive analytics and visualizations
- Automated pipeline orchestration
- Error handling and logging
- Configuration management

## License

This project is open source. Feel free to use and modify as needed.

## Support

For issues and questions:
1. Check the logs in `logs/` directory
2. Review the troubleshooting section
3. Open an issue with detailed error information

## Acknowledgments

- YouTube Data API v3 for providing video data
- Python data science ecosystem (pandas, matplotlib, seaborn)
- MySQL for reliable data storage