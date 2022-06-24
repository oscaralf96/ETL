# Utilities
import psycopg2


class Orm(object):

    def __init__(self, connection_params):
        self.__params = connection_params  # settings.Databases.get("ETL")
        self.__connection = None
        self.__cursor = None

    def connect(self):
        """Connect to postgreSQL database server"""
        try:
            # connect to database
            print(f"Connecting to {self.__params.get('database')} . . . ")
            self.__connection = psycopg2.connect(**self.__params)
            print("Connected")

            # Create cursor
            self.__get_cursor()

            print('PostgreSQL database version:')
            self.__cursor.execute('SELECT version()')

            # display the PostgreSQL database server version
            db_version = self.__cursor.fetchone()
            print(db_version)

            # close the communication with the PostgreSQL
            self.__close_cursor()

        except (Exception, psycopg2.DatabaseError) as error:
            print(error)
            if self.__connection is not None:
                self.disconnect()

    def disconnect(self):
        self.__connection.close()
        self.__connection = None
        print("Database connection is closed")

    def __get_cursor(self):
        self.__cursor = self.__connection.cursor()

    def __close_cursor(self):
        self.__cursor.close()
        self.__cursor = None

    def select_all(self, schema, table, **kwargs):
        self.__get_cursor()
        if kwargs["limit"]:
            self.__cursor.execute(f"SELECT * FROM {schema}.{table} LIMIT {kwargs['limit']}")
        else:
            self.__cursor.execute(f"SELECT * FROM {schema}.{table}")
        result = self.__cursor.fetchall()
        print(result)
        self.__close_cursor()




