import mysql.connector
import sys
import os

# Add parent directory to path to import config
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import DB_CONFIG

def setup_database():
    """Create database and tables"""
    try:
        # Connect without specifying database first
        temp_config = DB_CONFIG.copy()
        temp_config.pop('database', None)

        conn = mysql.connector.connect(**temp_config)
        cursor = conn.cursor()

        # Create database if it doesn't exist
        cursor.execute("CREATE DATABASE IF NOT EXISTS youtube_pipeline")
        print("✓ Database 'youtube_pipeline' created or already exists")

        # Switch to the database
        cursor.execute("USE youtube_pipeline")

        # Create youtube_videos table
        create_videos_table = """
        CREATE TABLE IF NOT EXISTS youtube_videos (
            id INT AUTO_INCREMENT PRIMARY KEY,
            video_id VARCHAR(20),
            title VARCHAR(255) NOT NULL,
            channel VARCHAR(255) NOT NULL,
            published_at DATETIME,
            views BIGINT DEFAULT 0,
            likes BIGINT DEFAULT 0,
            comments BIGINT DEFAULT 0,
            engagement_rate DECIMAL(10,6) DEFAULT 0.0,
            extracted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            INDEX idx_channel (channel),
            INDEX idx_views (views),
            INDEX idx_extracted_at (extracted_at)
        );
        """

        cursor.execute(create_videos_table)
        print("✓ Table 'youtube_videos' created or already exists")

        # Create pipeline_logs table
        create_logs_table = """
        CREATE TABLE IF NOT EXISTS pipeline_logs (
            id INT AUTO_INCREMENT PRIMARY KEY,
            stage VARCHAR(50) NOT NULL,
            status VARCHAR(20) NOT NULL,
            message TEXT,
            executed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            INDEX idx_stage (stage),
            INDEX idx_status (status)
        );
        """

        cursor.execute(create_logs_table)
        print("✓ Table 'pipeline_logs' created or already exists")

        conn.commit()
        print("✓ Database setup completed successfully!")

    except mysql.connector.Error as e:
        print(f"✗ MySQL error: {e}")
        return False
    except Exception as e:
        print(f"✗ Error: {e}")
        return False
    finally:
        if 'cursor' in locals():
            cursor.close()
        if 'conn' in locals():
            conn.close()

    return True

if __name__ == "__main__":
    success = setup_database()
    sys.exit(0 if success else 1)