import mysql.connector
import sys
import os

# Add parent directory to path to import config
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import DB_CONFIG

def check_database():
    """Check the contents of the YouTube videos database"""
    try:
        conn = mysql.connector.connect(**DB_CONFIG)
        cursor = conn.cursor()

        # Get total count
        cursor.execute("SELECT COUNT(*) FROM youtube_videos")
        count = cursor.fetchone()[0]
        print(f"Total videos in database: {count}")

        if count > 0:
            # Get sample videos
            cursor.execute("SELECT title, channel, views, likes, engagement_rate FROM youtube_videos LIMIT 5")
            results = cursor.fetchall()

            print("\nSample videos:")
            for row in results:
                title = row[0][:50] + "..." if len(row[0]) > 50 else row[0]
                print(f"- {title} by {row[1]}")
                print(f"  Views: {row[2]:,}, Likes: {row[3]:,}, Engagement: {row[4]:.4f}")

            # Get summary stats
            cursor.execute("""
                SELECT
                    AVG(views) as avg_views,
                    MAX(views) as max_views,
                    AVG(likes) as avg_likes,
                    AVG(engagement_rate) as avg_engagement,
                    COUNT(DISTINCT channel) as channels
                FROM youtube_videos
            """)
            stats = cursor.fetchone()

            print("\nDatabase Summary:")
            print(f"- Average views: {stats[0]:,.0f}")
            print(f"- Maximum views: {stats[1]:,.0f}")
            print(f"- Average likes: {stats[2]:,.0f}")
            print(f"- Average engagement rate: {stats[3]:.4f}")
            print(f"- Unique channels: {stats[4]}")

    except Exception as e:
        print(f"Error checking database: {e}")
    finally:
        if 'cursor' in locals():
            cursor.close()
        if 'conn' in locals():
            conn.close()

if __name__ == "__main__":
    check_database()