import psycopg2

# Database connection settings (match your Docker Compose).
# Should be in a .env file in production.
DB_SETTINGS = {
    "dbname": "postgres",
    "user": "postgres",
    "password": "1234",
    "host": "mascot-postgres",
    "port": 5432
}

conn = psycopg2.connect(**DB_SETTINGS)
cur = conn.cursor()