from airflow.hooks.postgres_hook import PostgresHook
import pandas as pd
from sqlalchemy import create_engine
from datetime import datetime


def csv_to_db(
        file_path: str,
        table_name: str,
        conn_string: str,
        schema_name="afl") -> None:
    """This functions read a csv file from a disc and inserts it into a database"""

    db_engine = create_engine(conn_string)
    df = pd.read_csv(file_path)
    df.to_sql(table_name, schema=schema_name, con=db_engine,
              index=False, chunksize=1000, method="multi", if_exists="append")


def get_metadata(ti) -> None:
    """This function obtains information about last round recorded into db"""

    current_year = str(datetime.today().year)
    postgres = PostgresHook(postgres_conn_id="afl_db")
    conn = postgres.get_conn()
    cursor = conn.cursor()
    query = f"select max(round) from afl.stg_total_results where year={current_year}"
    cursor.execute(query)
    result = cursor.fetchone()
    if result: 
        round = round[0]
    else:
        round = 1
    result = str(int(result) + 1)
    cursor.close()
    ti.xcom_push(key='current_year', value=current_year)
    ti.xcom_push(key='last_round', value=result)
