#utilities
import sqlalchemy, pandas as pd, numpy as np

import settings
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


if __name__ == '__main__':
    run()
