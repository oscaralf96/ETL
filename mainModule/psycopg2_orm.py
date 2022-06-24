# Utilities
import psycopg2
import sqlalchemy

# settings
import settings

class Orm(object):
    def __init__(self, connection_params):
        self.params = connection_params  # settings.Databases.get("ETL")
        self.connection = None
        self.cursor = None

        def connect():
            """Connect to postgreSQL database server"""
            conn = None
            params = settings.Databases.get("ETL")

            try:
                # connect to database
                print(f"Connecting to {params.get('database')} . . . ")
                conn = psycopg2.connect(**params)
                print("Connected")

                # Create cursor
                cursor = conn.cursor()

                print('PostgreSQL database version:')
                cursor.execute('SELECT version()')

                # display the PostgreSQL database server version
                db_version = cursor.fetchone()
                print(db_version)

                # close the communication with the PostgreSQL
                cursor.close()

            except (Exception, psycopg2.DatabaseError) as error:
                print(error)
            finally:
                if conn is not None:
                    conn.close()
                    print("Database connection is closed")

