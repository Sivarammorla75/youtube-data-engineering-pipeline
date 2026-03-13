-- YouTube Data Engineering Pipeline Database Schema

CREATE DATABASE IF NOT EXISTS youtube_pipeline;
USE youtube_pipeline;

CREATE TABLE IF NOT EXISTS youtube_videos (
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

-- Optional: Create a table for pipeline execution logs
CREATE TABLE IF NOT EXISTS pipeline_logs (
    id INT AUTO_INCREMENT PRIMARY KEY,
    stage VARCHAR(50) NOT NULL,
    status VARCHAR(20) NOT NULL,
    message TEXT,
    executed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_stage (stage),
    INDEX idx_status (status)
);