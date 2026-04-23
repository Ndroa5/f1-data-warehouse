from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import subprocess
import sys

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

def run_dim_driver():
    sys.path.insert(0, '/opt/airflow/project/gold')
    from sqlalchemy import create_engine
    from load import load_dim_driver
    engine = create_engine('postgresql://postgres:postgres123@172.21.0.1:5432/f1_warehouse')
    load_dim_driver(engine)

def run_dim_constructor():
    sys.path.insert(0, '/opt/airflow/project/gold')
    from sqlalchemy import create_engine
    from load import load_dim_constructor
    engine = create_engine('postgresql://postgres:postgres123@172.21.0.1:5432/f1_warehouse')
    load_dim_constructor(engine)

def run_dim_circuit():
    sys.path.insert(0, '/opt/airflow/project/gold')
    from sqlalchemy import create_engine
    from load import load_dim_circuit
    engine = create_engine('postgresql://postgres:postgres123@172.21.0.1:5432/f1_warehouse')
    load_dim_circuit(engine)

def run_dim_status():
    sys.path.insert(0, '/opt/airflow/project/gold')
    from sqlalchemy import create_engine
    from load import load_dim_status
    engine = create_engine('postgresql://postgres:postgres123@172.21.0.1:5432/f1_warehouse')
    load_dim_status(engine)

def run_dim_date():
    sys.path.insert(0, '/opt/airflow/project/gold')
    from sqlalchemy import create_engine
    from load import load_dim_date
    engine = create_engine('postgresql://postgres:postgres123@172.21.0.1:5432/f1_warehouse')
    load_dim_date(engine)

def run_dim_race():
    sys.path.insert(0, '/opt/airflow/project/gold')
    from sqlalchemy import create_engine
    from load import load_dim_race
    engine = create_engine('postgresql://postgres:postgres123@172.21.0.1:5432/f1_warehouse')
    load_dim_race(engine)

def run_fact_results():
    sys.path.insert(0, '/opt/airflow/project/gold')
    from sqlalchemy import create_engine
    from load import load_fact_results
    engine = create_engine('postgresql://postgres:postgres123@172.21.0.1:5432/f1_warehouse')
    load_fact_results(engine)

def run_fact_lap_times():
    sys.path.insert(0, '/opt/airflow/project/gold')
    from sqlalchemy import create_engine
    from load import load_fact_lap_times
    engine = create_engine('postgresql://postgres:postgres123@172.21.0.1:5432/f1_warehouse')
    load_fact_lap_times(engine)

def run_fact_pit_stops():
    sys.path.insert(0, '/opt/airflow/project/gold')
    from sqlalchemy import create_engine
    from load import load_fact_pit_stops
    engine = create_engine('postgresql://postgres:postgres123@172.21.0.1:5432/f1_warehouse')
    load_fact_pit_stops(engine)

def run_fact_driver_standings():
    sys.path.insert(0, '/opt/airflow/project/gold')
    from sqlalchemy import create_engine
    from load import load_fact_driver_standings
    engine = create_engine('postgresql://postgres:postgres123@172.21.0.1:5432/f1_warehouse')
    load_fact_driver_standings(engine)

def run_fact_constructor_standings():
    sys.path.insert(0, '/opt/airflow/project/gold')
    from sqlalchemy import create_engine
    from load import load_fact_constructor_standings
    engine = create_engine('postgresql://postgres:postgres123@172.21.0.1:5432/f1_warehouse')
    load_fact_constructor_standings(engine)

with DAG(
    dag_id="f1_etl_pipeline",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False
) as dag:

    bronze_task = PythonOperator(task_id="load_bronze", python_callable=run_bronze)
    silver_task = PythonOperator(task_id="load_silver", python_callable=run_silver)

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

    # Lanac
    bronze_task >> silver_task

    # Paralelne DIM tabele
    silver_task >> [dim_driver_task, dim_constructor_task, dim_circuit_task, dim_status_task, dim_date_task]

    # dim_race čeka sve DIM tabele
    [dim_driver_task, dim_constructor_task, dim_circuit_task, dim_status_task, dim_date_task] >> dim_race_task

    # Paralelne FACT tabele
    dim_race_task >> [fact_results_task, fact_lap_times_task, fact_pit_stops_task, fact_driver_standings_task, fact_constructor_standings_task]
