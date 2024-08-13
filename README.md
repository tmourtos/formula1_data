# Formula 1 Data collector

Welcome to the Formula 1 Data collector project. 
This Python project utilizes the [Ergast Developer API](https://ergast.com/mrd/) 
to collect Formula 1 (F1) racing data. It then performs essential 
data transformations and persists the transformed data into a 
relational database hosted on Microsoft Azure.

This script is designed for:
* F1 enthusiasts who want to collect and analyze race data.
* Developers seeking to explore data acquisition from APIs and 
data storage in Azure databases.

## Table of Contents
- [Data Sources](#data-sources)
- [Installation](#installation)
- [Usage](#usage)

## Data Sources

### Ergast API
The Ergast Motorsports API is the primary data source. 
The Ergast API provides a comprehensive historical record of 
Formula 1 data, including information about:

* Constructors: Teams and their history
* Drivers: Driver profiles and career statistics
* Races: Race schedules, results, and qualifying data
* Circuits: Track information and locations
* Standings: Driver and constructor championship standings
* Lap times: Detailed lap data for each race
* Pit stops: Pit stop information for each race

By leveraging the Ergast API, this script accesses a rich dataset of F1 information to populate the Azure database.

Note: The Ergast API is a valuable resource for non-commercial F1 data analysis projects. Please refer to the API's terms of use for specific guidelines and limitations.

## Installation

### Prerequisites
* Python: Ensure you have Python installed on your system.
* Airflow: This project utilizes Airflow for workflow orchestration. Please refer to the Airflow documentation for installation instructions.

### Dependencies
The required Python packages are listed in the requirements.txt file. To install them, run the following command:

```
pip install -r requirements.txt
```

### Environment Variables
Before running the script, you must set the following environment variables:

* DB_SERVER: The host server.
* DB_NAME: The database name.
* DB_PASSWORD: The database password.
* DB_PORT: The database port.
* DB_USERNAME: The database username.

Important: For security reasons, it's strongly recommended to use environment variables instead of hardcoding database credentials directly in the script.

## Usage

Airflow is utilized for orchestration, so it interacts through 
the Airflow web UI or command-line interface (CLI).

### Using the Airflow Web UI:

Access the Airflow Web UI: 
* Navigate to the Airflow web interface's URL 
in the web browser. You'll often find it [here](http://<your-airflow-webserver-address>:8080).
* Locate the DAG: In the Airflow UI, search for the DAG named f1_dag.
* Trigger the DAG: Click the "Trigger DAG" button next to the f1_dag entry. 
This will initiate the data collection process as defined

### Using the Airflow CLI:

* Open Terminal: Launch the terminal or command prompt.
* Navigate to Airflow Home: Use the cd command to reach the directory 
containing the Airflow configuration file (airflow.cfg).
* Trigger the DAG: Execute the following command, replacing <dag_id> 
with the actual name of your DAG:

```
airflow dags trigger <dag_id>
```

#### Additional Notes:

The schedule_interval=None setting in your DAG ensures it won't run 
automatically. Manual triggering is required using the methods above.