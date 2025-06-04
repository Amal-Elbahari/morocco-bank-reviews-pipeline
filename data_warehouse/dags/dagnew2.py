from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import sys
import os
sys.path.append('/home/amal/airflow/scriptsnew')
from export_to_csv import export_tables_to_csv


# Ajouter le chemin vers ton script nlp
sys.path.append('/home/amal/airflow/scriptsnew')
from nlp_enrichment import enrich_reviews

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
    'start_date': datetime(2024, 4, 1),
}

with DAG(
    dag_id='bank',
    default_args=default_args,
    description='ETL pipeline for bank reviews',
    schedule_interval='@daily',
    catchup=False,
    tags=['bank', 'ETL'],
) as dag:

    extract = BashOperator(
        task_id='extract_data',
        bash_command='python3 /home/amal/airflow/scriptsnew/scrap.py'
    )

    load = BashOperator(
        task_id='load_data',
        bash_command='python3 /home/amal/airflow/scriptsnew/load.py'
    )

    enrich = PythonOperator(
        task_id='nlp_enrichment',
        python_callable=enrich_reviews
    )

    dbt_run = BashOperator(
        task_id='run_dbt',
        bash_command='cd /mnt/c/Users/elbah/dw_reviews_project && dbt run'
    )
    export_csv = PythonOperator(
    task_id='export_csv',
    python_callable=export_tables_to_csv
)


    # Ordre : extract → load → enrich → dbt
    extract >> load >> enrich >> dbt_run >> export_csv
