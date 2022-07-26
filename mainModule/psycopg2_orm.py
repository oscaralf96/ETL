# Utilities
import psycopg2
import platform

#psycopg2 errors
from psycopg2.errors import UndefinedTable, UndefinedColumn, InvalidTextRepresentation


class Orm(object):

    def __init__(self, connection_params):
        self.__params = connection_params  # settings.Databases.get("ETL")
        if platform.system() == "Linux":
            self.__params["database"] = self.__params["database"].lower()
        self.__connection = None
        self.__cursor = None

    def connect(self):
        """Connect to postgreSQL database server"""
        try:
            # connect to database
            print(f"Connecting to {self.__params.get('database')} . . . ")
            self.__connection = psycopg2.connect(**self.__params)
            print("Connected")

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

    def get_version(self):
        self.__get_cursor()
        self.__cursor.execute('SELECT version()')
        print('PostgreSQL database version:')
        print(self.__cursor.fetchone())

    def select(self, schema, table, **kwargs):
        self.__get_cursor()
        try:
            query = f"SELECT * FROM {schema}.{table}"
            if "orderby" in kwargs.keys():
                query = query + f" ORDER BY {kwargs['orderby']}"
            if "limit" in kwargs.keys():
                query = query + f" LIMIT {kwargs['limit']}"
            self.__cursor.execute(query)
            result = self.__cursor.fetchall()
            print(result)
        except UndefinedTable:
            print("Table or schema doesn't exist")
        finally:
            self.__connection.rollback()
            self.__close_cursor()

    def filter(self, schema, table, column, **kwargs):
        self.__get_cursor()
        try:
            query = f"SELECT * FROM {schema}.{table} WHERE {column}"
            if "search" in kwargs.keys():
                query = query + f" ={kwargs['search']}"
            elif "like" in kwargs.keys():
                query = query + f" LIKE '{kwargs['like']}'"
            elif "notlike" in kwargs.keys():
                query = query + f" NOT LIKE '{kwargs['notlike']}'"
            if "orderby" in kwargs.keys():
                query = query + f" ORDER BY {kwargs['orderby']}"
            if "limit" in kwargs.keys():
                query = query + f" LIMIT {kwargs['limit']}"
            self.__cursor.execute(query)
            result = self.__cursor.fetchall()
            print(result)
        except UndefinedTable:
            print("Table or schema doesn't exist")
        except UndefinedColumn:
            print("Column doesn't exist")
        except InvalidTextRepresentation:
            print("Field type 'Integer' you search by 'text'")
        finally:
            self.__connection.rollback()
            self.__close_cursor()

