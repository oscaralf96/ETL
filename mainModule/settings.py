

# DataBase configuration

Databases = {
    "ETL": {
        # "engine": "postgresql", # Needed with sqlalchemy
        # "driver": "psycopg2", # Needed with sqlalchemy
        "host": "localhost",
        "database": "cursoPracticoPlatzi",
        # "user": "usuario_consulta", # Config for sqlalchemy
        "user": "psycopg2",
        "password": "PassW0rd!",
        "port": "5432"
    }
}