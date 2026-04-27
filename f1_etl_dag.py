from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import subprocess
import sys
import os
from dotenv import load_dotenv

load_dotenv('/opt/airflow/project/.env')

DB_URL = (
    f"postgresql://{os.getenv('DB_USER', 'postgres')}:{os.getenv('DB_PASSWORD', 'postgres123')}"
    f"@{os.getenv('DB_HOST', '172.21.0.1')}:{os.getenv('DB_PORT', '5432')}/{os.getenv('DB_NAME', 'f1_warehouse')}"
)

def run_bronze():
    subprocess.run([
        "python3",
        "/opt/airflow/project/bronze/main.py"
    ], check=True)

def run_silver():
    subprocess.run([
        "python3",
        "/opt/airflow/project/silver/main.py"
    ], check=True)

def run_silver_dq_checks_task():
    sys.path.insert(0, '/opt/airflow/project/silver')
    from sqlalchemy import create_engine
    from dq_checks import run_silver_dq_checks
    engine = create_engine(DB_URL)
    run_silver_dq_checks(engine)

def run_dim_driver():
    sys.path.insert(0, '/opt/airflow/project/gold')
    from sqlalchemy import create_engine
    from load import load_dim_driver
    engine = create_engine(DB_URL)
    load_dim_driver(engine)

def run_dim_constructor():
    sys.path.insert(0, '/opt/airflow/project/gold')
    from sqlalchemy import create_engine
    from load import load_dim_constructor
    engine = create_engine(DB_URL)
    load_dim_constructor(engine)

def run_dim_circuit():
    sys.path.insert(0, '/opt/airflow/project/gold')
    from sqlalchemy import create_engine
    from load import load_dim_circuit
    engine = create_engine(DB_URL)
    load_dim_circuit(engine)

def run_dim_status():
    sys.path.insert(0, '/opt/airflow/project/gold')
    from sqlalchemy import create_engine
    from load import load_dim_status
    engine = create_engine(DB_URL)
    load_dim_status(engine)

def run_dim_date():
    sys.path.insert(0, '/opt/airflow/project/gold')
    from sqlalchemy import create_engine
    from load import load_dim_date
    engine = create_engine(DB_URL)
    load_dim_date(engine)

def run_dim_race():
    sys.path.insert(0, '/opt/airflow/project/gold')
    from sqlalchemy import create_engine
    from load import load_dim_race
    engine = create_engine(DB_URL)
    load_dim_race(engine)

def run_fact_results():
    sys.path.insert(0, '/opt/airflow/project/gold')
    from sqlalchemy import create_engine
    from load import load_fact_results
    engine = create_engine(DB_URL)
    load_fact_results(engine)

def run_fact_lap_times():
    sys.path.insert(0, '/opt/airflow/project/gold')
    from sqlalchemy import create_engine
    from load import load_fact_lap_times
    engine = create_engine(DB_URL)
    load_fact_lap_times(engine)

def run_fact_pit_stops():
    sys.path.insert(0, '/opt/airflow/project/gold')
    from sqlalchemy import create_engine
    from load import load_fact_pit_stops
    engine = create_engine(DB_URL)
    load_fact_pit_stops(engine)

def run_fact_driver_standings():
    sys.path.insert(0, '/opt/airflow/project/gold')
    from sqlalchemy import create_engine
    from load import load_fact_driver_standings
    engine = create_engine(DB_URL)
    load_fact_driver_standings(engine)

def run_fact_constructor_standings():
    sys.path.insert(0, '/opt/airflow/project/gold')
    from sqlalchemy import create_engine
    from load import load_fact_constructor_standings
    engine = create_engine(DB_URL)
    load_fact_constructor_standings(engine)

def run_dq_checks_task():
    sys.path.insert(0, '/opt/airflow/project/gold')
    from sqlalchemy import create_engine
    from dq_checks import run_dq_checks
    engine = create_engine(DB_URL)
    run_dq_checks(engine)

def run_kafka_producer():
    sys.path.insert(0, '/opt/airflow/project/kafka')
    from sqlalchemy import create_engine
    from producer import get_producer, stream_lap_times, stream_pit_stops, stream_results
    engine = create_engine(DB_URL)
    producer = get_producer()
    stream_lap_times(producer, engine)
    stream_pit_stops(producer, engine)
    stream_results(producer, engine)
    producer.flush()

def run_kafka_consumer():
    sys.path.insert(0, '/opt/airflow/project/kafka')
    from sqlalchemy import create_engine
    from consumer import create_kafka_tables, consume_messages
    engine = create_engine(DB_URL)
    create_kafka_tables(engine)
    consume_messages(engine)

with DAG(
    dag_id="f1_etl_pipeline",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False
) as dag:

    bronze_task = PythonOperator(task_id="load_bronze", python_callable=run_bronze)
    silver_task = PythonOperator(task_id="load_silver", python_callable=run_silver)
    silver_dq_task = PythonOperator(task_id="silver_dq_checks", python_callable=run_silver_dq_checks_task)

    dim_driver_task = PythonOperator(task_id="dim_driver", python_callable=run_dim_driver)
    dim_constructor_task = PythonOperator(task_id="dim_constructor", python_callable=run_dim_constructor)
    dim_circuit_task = PythonOperator(task_id="dim_circuit", python_callable=run_dim_circuit)
    dim_status_task = PythonOperator(task_id="dim_status", python_callable=run_dim_status)
    dim_date_task = PythonOperator(task_id="dim_date", python_callable=run_dim_date)
    dim_race_task = PythonOperator(task_id="dim_race", python_callable=run_dim_race)

    fact_results_task = PythonOperator(task_id="fact_results", python_callable=run_fact_results)
    fact_lap_times_task = PythonOperator(task_id="fact_lap_times", python_callable=run_fact_lap_times)
    fact_pit_stops_task = PythonOperator(task_id="fact_pit_stops", python_callable=run_fact_pit_stops)
    fact_driver_standings_task = PythonOperator(task_id="fact_driver_standings", python_callable=run_fact_driver_standings)
    fact_constructor_standings_task = PythonOperator(task_id="fact_constructor_standings", python_callable=run_fact_constructor_standings)

    dq_checks_task = PythonOperator(task_id="dq_checks", python_callable=run_dq_checks_task)
kafka_producer_task = PythonOperator(task_id="kafka_producer", python_callable=run_kafka_producer)
kafka_consumer_task = PythonOperator(task_id="kafka_consumer", python_callable=run_kafka_consumer)


    # Lanac
    bronze_task >> silver_task >> silver_dq_task

    # Paralelne DIM tabele čekaju silver DQ
    silver_dq_task >> [dim_driver_task, dim_constructor_task, dim_circuit_task, dim_status_task, dim_date_task]

    # dim_race čeka sve DIM tabele
    [dim_driver_task, dim_constructor_task, dim_circuit_task, dim_status_task, dim_date_task] >> dim_race_task

    # Paralelne FACT tabele
    dim_race_task >> [fact_results_task, fact_lap_times_task, fact_pit_stops_task, fact_driver_standings_task, fact_constructor_standings_task]

    # Gold DQ checks čekaju sve FACT tabele
    [fact_results_task, fact_lap_times_task, fact_pit_stops_task, fact_driver_standings_task, fact_constructor_standings_task] >> dq_checks_task >> kafka_producer_task >> kafka_consumer_task
