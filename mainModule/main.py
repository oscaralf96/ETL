#utilities
from cgitb import reset
import datetime

import sqlalchemy, pandas as pd, numpy as np

import settings
from mainModule.functions import get_month
from psycopg2_orm import Orm


def get_engine(engine, driver, user, password, host, port, database):
    eng = None
    try:
        eng = sqlalchemy.create_engine(
            url=f"{engine}+{driver}://{user}:{password}@{host}/{database}",
            echo=False
        )
    except ValueError as e:
        print(e)
    finally:
        return eng


def run_sqlalchemy():
    print("------Inicio del programa------")
    engine = get_engine(**settings.Databases.get("ETL"))
    connection = engine.connect()
    metadata = sqlalchemy.MetaData(schema="platzi")
    alumnos = sqlalchemy.Table("alumnos", metadata, autoload=True, autoload_with=engine)

    # print(alumnos.columns.keys())

    query = sqlalchemy.select([alumnos])
    resultProxy = connection.execute(query)
    resultSet = resultProxy.fetchall()
    # print(resultSet[:10])
    df_alumnos = pd.DataFrame(resultSet)
    df_alumnos.columns = resultSet[0].keys()

    print(df_alumnos.head())


def run():
    orm = Orm(settings.Databases.get("ETL"))
    orm.connect()
    orm.get_version()
    print("select")
    orm.select(schema='platzi', table='alumnos', orderby="nombre", limit=2)
    print("where")
    orm.filter(schema="platzi", table="alumnos", column="id", search="3", orderby="nombre")
    print("like")
    orm.filter(schema="platzi", table="alumnos", column="nombre", like="H%", orderby="nombre", limit=2)
    print("not like")
    orm.filter(schema="platzi", table="alumnos", column="nombre", notlike="H%", orderby="nombre", limit=2)
    orm.disconnect()


def dashboard_view_analytics():
    import pymssql

    today = datetime.datetime.now()

    today = datetime.datetime.now()

    conn = pymssql.connect(server="10.24.11.23", user="project", password="project", database="ZMAP")
    cursor = conn.cursor()

    query = {}

    # Seleccionar años desde vista v_anios
    cursor.execute(f'SELECT * FROM v_anios;')
    years = [year[0] for year in cursor.fetchall()]
    years.sort()

    query['years'] = {}
    for year in years:
        cursor.execute(f'EXEC cantidadesAnaliticas @anio={year}, @mes=NULL, @depto=NULL, @muni=NULL')
        year_data = [[int(query_item[0]), round(query_item[1], 2), query_item[2]] for query_item in cursor.fetchall()][
            0]
        query['years'][f'{year}'] = {
            'cylinders': year_data[0],
            'gallons': year_data[1],
            'cefs': year_data[2]
        }

    gallons = {}
    for year in range(2021, today.year + 1):
        gallons[f'{year}'] = {}
        for month in range(1, today.month + 1):
            cursor.execute(f'EXEC cantidadGalones @anio={year}, @mes={month}, @depto=NULL, @muni=NULL')
            gallons[f'{year}'][f'{month}'] = round(cursor.fetchall()[0][0], 2)

    query['gallons'] = gallons

    cursor.execute(f'EXEC cantidadesAnaliticas @anio={today.year}, @mes={today.month}, @depto=NULL, @muni=NULL')
    this_month_data = [[int(query_item[0]), round(query_item[1], 2), query_item[2]] for query_item in cursor.fetchall()][
        0]
    query['this_month'] = {
            'cylinders': this_month_data[0],
            'gallons': this_month_data[1],
            'cefs': this_month_data[2]
        }

    this_month_cylinders = query['this_month']['cylinders']
    this_month_gallons = query['this_month']['gallons']
    this_month_cefs = query['this_month']['cefs']

    this_year_cylinders = query['years'][f'{today.year}']['cylinders']
    this_year_gallons = query['years'][f'{today.year}']['gallons']
    this_year_cefs = query['years'][f'{today.year}']['cefs']

    print(query)
    print({
        'this_month_cylinders': this_month_cylinders,
        'this_month_gallons': this_month_gallons,
        'this_month_cefs': this_month_cefs,
        'this_year_cylinders': this_year_cylinders,
        'this_year_gallons': this_year_gallons,
        'this_year_cefs': this_year_cefs,
    })


def weekly_view_query():
    import pymssql

    today = datetime.datetime.now()

    today = datetime.datetime.now()

    conn = pymssql.connect(server="10.24.11.23", user="project", password="project", database="ZMAP")
    cursor = conn.cursor()

    query = {}

    # Seleccionar desde vista ventas_ultimos7dias
    cursor.execute(f"SELECT fecha, SUM(galones) as Galones, SUM(Cilindros) as Cilindros FROM ventas_ultimos7dias WHERE fecha > '{today.year}-{today.month}-{today.day - 7}' group by fecha;")  # WHERE fecha >= '2022-07-16'
    result = [(query_item[0], round(query_item[1], 2), int(query_item[2])) for query_item in cursor.fetchall()]
    print([name[0] for name in cursor.description])
    for i in result:
        print(i)
    print(type(result[0][0]))


if __name__ == '__main__':
    weekly_view_query()


