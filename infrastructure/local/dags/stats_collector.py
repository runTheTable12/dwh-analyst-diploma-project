from datetime import datetime

from airflow import DAG
from airflow.models import Variable
from airflow.operators.sql import BranchSQLOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator

from record_to_db.transfer_functions import csv_to_db, get_metadata

afl_conn = 'postgresql://analyst:analyst@172.17.0.1:5432/afl-db'
total_results_path = '/opt/airflow/total_results.csv'
round_results_path = '/opt/airflow/round_results.csv'
total_tips_path = '/opt/airflow/total_tips.csv'
round_tips_path = '/opt/airflow/round_tips.csv'
player_stats_path = '/opt/airflow/player_stats.csv'
round_stats_path = '/opt/airflow/round_stats.csv'

TIPS_TABLE_NAME = 'stg_total_tips'
RESULTS_TABLE_NAME = 'stg_total_results'
PLAYER_STATS_TABLE_NAME = 'stg_player_stats'


with DAG(
    dag_id="stats_collector",
    start_date=datetime(2022, 1, 1),
    schedule_interval="@weekly",
    catchup=False,
) as dag:

    check_schema = BranchSQLOperator(
        task_id="check_schema",
        sql="""
            SELECT count(1) FROM information_schema.schemata where schema_name = 'afl';
          """,
        conn_id='afl_db',
        follow_task_ids_if_true="get_metadata",
        follow_task_ids_if_false="get_all_data"
    )
    start_dag = DummyOperator(task_id='start_dag')

    end_dag_initial = DummyOperator(task_id='end_dag_initial')
    end_dag_regular = DummyOperator(task_id='end_dag_regular')

    update_data = BashOperator(
        task_id='update_data',
        bash_command="""Rscript /opt/airflow/scripts/collect_new.R {{ ti.xcom_pull(task_ids="get_metadata", key="current_year") }} {{ ti.xcom_pull(task_ids="get_metadata", key="last_round") }}""")

    get_metadata = PythonOperator(
        task_id="get_metadata",
        python_callable=get_metadata)

    update_results = PythonOperator(
        task_id="update_results",
        python_callable=csv_to_db,
        op_kwargs={
            'file_path': round_results_path,
            'table_name': RESULTS_TABLE_NAME,
            'conn_string': afl_conn})

    update_tips = PythonOperator(
        task_id="update_tips",
        python_callable=csv_to_db,
        op_kwargs={
            'file_path': round_tips_path,
            'table_name': TIPS_TABLE_NAME,
            'conn_string': afl_conn})

    update_stats = PythonOperator(
        task_id="update_stats",
        python_callable=csv_to_db,
        op_kwargs={
            'file_path': round_stats_path,
            'table_name': PLAYER_STATS_TABLE_NAME,
            'conn_string': afl_conn})

    create_afl_schema = PostgresOperator(task_id="create_afl_schema",
                                         sql="""create schema afl;""",
                                         postgres_conn_id="afl_db")

    get_all_data = BashOperator(
        task_id='get_all_data',
        bash_command="Rscript /opt/airflow/scripts/get_data.R")

    download_results = PythonOperator(
        task_id="download_results",
        python_callable=csv_to_db,
        op_kwargs={
            'file_path': total_results_path,
            'table_name': RESULTS_TABLE_NAME,
            'conn_string': afl_conn})

    download_tips = PythonOperator(
        task_id="download_tips",
        python_callable=csv_to_db,
        op_kwargs={
            'file_path': total_tips_path,
            'table_name': TIPS_TABLE_NAME,
            'conn_string': afl_conn})

    download_stats = PythonOperator(
        task_id="download_stats",
        python_callable=csv_to_db,
        op_kwargs={
            'file_path': player_stats_path,
            'table_name': PLAYER_STATS_TABLE_NAME,
            'conn_string': afl_conn})

    start_dag >> check_schema >> [get_metadata, get_all_data]

    get_metadata >> update_data >> [update_results,
                                    update_tips,
                                    update_stats] >> end_dag_regular

    get_all_data >> create_afl_schema >> [download_results,
                                          download_tips,
                                          download_stats] >> end_dag_initial
