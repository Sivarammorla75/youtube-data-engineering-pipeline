import mysql.connector
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import logging
import os
import sys
from datetime import datetime, timedelta

# Add parent directory to path to import config
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import DB_CONFIG, LOG_LEVEL, LOG_FILE

# Setup logging
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL),
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(LOG_FILE),
        logging.StreamHandler()
    ]
)

# Set style for better plots
plt.style.use('default')
sns.set_palette("husl")

def analyze_youtube_data():
    """Analyze YouTube data with comprehensive metrics and visualizations"""
    try:
        logging.info("Starting YouTube data analysis")

        conn = mysql.connector.connect(**DB_CONFIG)
        cursor = conn.cursor()

        # Check if data exists
        cursor.execute("SELECT COUNT(*) FROM youtube_videos")
        count = cursor.fetchone()[0]

        if count == 0:
            logging.warning("No data found in youtube_videos table")
            return False

        logging.info(f"Analyzing {count} video records")

        # Create output directory for plots
        plots_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), "data", "plots")
        os.makedirs(plots_dir, exist_ok=True)

        # Analysis 1: Top channels by views
        query1 = """
        SELECT channel, SUM(views) as total_views, COUNT(*) as video_count,
               AVG(engagement_rate) as avg_engagement
        FROM youtube_videos
        GROUP BY channel
        ORDER BY total_views DESC
        LIMIT 10
        """

        df_top_channels = pd.read_sql(query1, conn)

        plt.figure(figsize=(12, 6))
        bars = plt.bar(range(len(df_top_channels)), df_top_channels['total_views'])
        plt.xticks(range(len(df_top_channels)), df_top_channels['channel'], rotation=45, ha='right')
        plt.title('Top 10 YouTube Channels by Total Views')
        plt.xlabel('Channel')
        plt.ylabel('Total Views')
        plt.tight_layout()
        plt.savefig(f"{plots_dir}/top_channels_views.png", dpi=300, bbox_inches='tight')
        plt.close()

        # Analysis 2: Engagement rate distribution
        query2 = """
        SELECT engagement_rate, views, likes
        FROM youtube_videos
        WHERE views > 0
        ORDER BY engagement_rate DESC
        LIMIT 1000
        """

        df_engagement = pd.read_sql(query2, conn)

        plt.figure(figsize=(10, 6))
        plt.hist(df_engagement['engagement_rate'], bins=50, alpha=0.7, edgecolor='black')
        plt.title('Distribution of Engagement Rates')
        plt.xlabel('Engagement Rate (likes + comments / views)')
        plt.ylabel('Frequency')
        plt.tight_layout()
        plt.savefig(f"{plots_dir}/engagement_distribution.png", dpi=300, bbox_inches='tight')
        plt.close()

        # Analysis 3: Views vs Engagement scatter plot
        plt.figure(figsize=(10, 6))
        plt.scatter(df_engagement['views'], df_engagement['engagement_rate'], alpha=0.6)
        plt.title('Views vs Engagement Rate')
        plt.xlabel('Views')
        plt.ylabel('Engagement Rate')
        plt.tight_layout()
        plt.savefig(f"{plots_dir}/views_vs_engagement.png", dpi=300, bbox_inches='tight')
        plt.close()

        # Analysis 4: Summary statistics
        query3 = """
        SELECT
            COUNT(*) as total_videos,
            AVG(views) as avg_views,
            MAX(views) as max_views,
            AVG(likes) as avg_likes,
            MAX(likes) as max_likes,
            AVG(engagement_rate) as avg_engagement,
            COUNT(DISTINCT channel) as unique_channels
        FROM youtube_videos
        """

        df_stats = pd.read_sql(query3, conn)

        # Print summary statistics
        print("\n" + "="*50)
        print("YOUTUBE DATA ANALYSIS SUMMARY")
        print("="*50)
        print(f"Total Videos Analyzed: {df_stats.iloc[0]['total_videos']:,}")
        print(f"Unique Channels: {df_stats.iloc[0]['unique_channels']:,}")
        print(f"Average Views per Video: {df_stats.iloc[0]['avg_views']:,.0f}")
        print(f"Maximum Views: {df_stats.iloc[0]['max_views']:,.0f}")
        print(f"Average Likes per Video: {df_stats.iloc[0]['avg_likes']:,.0f}")
        print(f"Average Engagement Rate: {df_stats.iloc[0]['avg_engagement']:.4f}")
        print("="*50)

        # Save summary to file
        summary_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), "data", "analysis_summary.txt")
        with open(summary_path, "w") as f:
            f.write("YouTube Data Analysis Summary\n")
            f.write(f"Generated on: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")
            f.write(f"Total Videos: {df_stats.iloc[0]['total_videos']:,}\n")
            f.write(f"Unique Channels: {df_stats.iloc[0]['unique_channels']:,}\n")
            f.write(f"Average Views: {df_stats.iloc[0]['avg_views']:,.0f}\n")
            f.write(f"Max Views: {df_stats.iloc[0]['max_views']:,.0f}\n")
            f.write(f"Average Likes: {df_stats.iloc[0]['avg_likes']:,.0f}\n")
            f.write(f"Average Engagement Rate: {df_stats.iloc[0]['avg_engagement']:.4f}\n")

        logging.info("Analysis completed successfully. Plots saved to data/plots/")
        return True

    except Exception as e:
        logging.error(f"Error in analyze_youtube_data: {str(e)}")
        return False
    finally:
        if 'cursor' in locals():
            cursor.close()
        if 'conn' in locals():
            conn.close()

if __name__ == "__main__":
    success = analyze_youtube_data()
    if success:
        print("\nAnalysis completed! Check the data/plots/ directory for visualizations.")
    sys.exit(0 if success else 1)