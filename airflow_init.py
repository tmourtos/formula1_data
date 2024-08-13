from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

from f1 import *

arguments = {
    'owner': 'airflow_f1',
    'start_date': datetime.now(),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='f1_dag',
    default_args=arguments,
    schedule_interval=None,
) as dag:

    # Define the Python function to call
    def call_external_procedures():
        seasons_data = SeasonCollector()
        seasons_data.run()
        drivers_data = DriverCollector()
        drivers_data.run()
        constructors_data = ConstructorCollector()
        constructors_data.run()
        status_data = StatusCollector()
        status_data.run()
        circuits_data = CircuitCollector()
        circuits_data.run()
        races_data = RaceCollector()
        races_data.run()
        driver_standings_data = DriverStandingsCollector()
        driver_standings_data.run()
        constructor_standings_data = ConstructorStandingsCollector()
        constructor_standings_data.run()
        qualifying_data = QualifyingCollector()
        qualifying_data.run()
        pit_stops_data = PitStopsCollector()
        pit_stops_data.run()
        laps_data = LapTimesCollector()
        laps_data.run()
        results_data = ResultsCollector()
        results_data.run()
        sprint_results_data = SprintResultsCollector()
        sprint_results_data.run()

        # You can return any results or perform additional tasks here

    # Define the Airflow task
    run_procedures = PythonOperator(
        task_id='run_procedures',
        python_callable=call_external_procedures,
        provide_context=True,  # Optional, pass context to the function
    )
