import mysql.connector

# Connect to MySQL
connection = mysql.connector.connect(
    host="localhost",
    user="root",
    password="Amma@1234#",
    database="youtube_pipeline"
)

print("Connected to MySQL successfully!")

# Create cursor to run SQL queries
cursor = connection.cursor()

cursor.execute("SELECT DATABASE();")

result = cursor.fetchone()

print("Connected to database:", result)