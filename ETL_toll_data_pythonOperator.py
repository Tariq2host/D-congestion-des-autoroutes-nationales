from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
import tarfile
import os
import csv

# Define default arguments for the DAG
default_args = {
    'owner': 'dummy_name',  
    'start_date': datetime.now(),
    'email': ['dummy_email@example.com'], 
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


# Initialize the DAG
dag = DAG(
    'ETL_toll_data',
    default_args=default_args,
    description='A simple ETL DAG for toll data',
    schedule_interval=timedelta(days=1),
)

# Function to unzip data
def unzip_data_function():
    source_file = '/home/project/airflow/dags/finalassignment/tolldata.tgz' 
    destination_dir = '/home/project/airflow/dags/finalassignment/unzipped_data' 

    if not os.path.exists(destination_dir):
        os.makedirs(destination_dir)

    with tarfile.open(source_file, 'r:gz') as tar:
        tar.extractall(path=destination_dir)

# Task to unzip data
unzip_data = PythonOperator(
    task_id='unzip_data',
    python_callable=unzip_data_function,
    dag=dag,
)

import logging

# Function to extract specific fields from CSV
def extract_data_from_csv_function():
    input_csv = '/home/project/airflow/dags/finalassignment/unzipped_data/vehicle-data.csv'  
    output_csv = '/home/project/airflow/dags/finalassignment/unzipped_data/csv_data.csv' 

    logging.info(f"Reading from {input_csv}")
    logging.info(f"Writing to {output_csv}")

    try:
        with open(input_csv, 'r') as infile, open(output_csv, 'w', newline='') as outfile:
            reader = csv.DictReader(infile)
            fieldnames = ['Rowid', 'Timestamp', 'Anonymized Vehicle number', 'Vehicle type']
            writer = csv.DictWriter(outfile, fieldnames=fieldnames)

            writer.writeheader()
            for row in reader:
                filtered_row = {field: row[field] for field in fieldnames}
                writer.writerow(filtered_row)
                logging.info(f"Processed row: {filtered_row}")
    except Exception as e:
        logging.error(f"Error processing CSV: {e}")
        raise

# Task to extract data from CSV
extract_data_from_csv = PythonOperator(
    task_id='extract_data_from_csv',
    python_callable=extract_data_from_csv_function,
    dag=dag,
)


# Function to extract specific fields from TSV
def extract_data_from_tsv_function():
    input_tsv = '/home/project/airflow/dags/finalassignment/unzipped_data/tollplaza-data.tsv'  
    output_csv = '/home/project/airflow/dags/finalassignment/unzipped_data/tsv_data.csv' 

    with open(input_tsv, 'r') as infile, open(output_csv, 'w', newline='') as outfile:
        reader = csv.DictReader(infile, delimiter='\t')
        fieldnames = ['Number of axles', 'Tollplaza id', 'Tollplaza code']
        writer = csv.DictWriter(outfile, fieldnames=fieldnames)

        writer.writeheader()
        for row in reader:
            filtered_row = {field: row[field] for field in fieldnames}
            writer.writerow(filtered_row)


# Task to extract data from TSV
extract_data_from_tsv = PythonOperator(
    task_id='extract_data_from_tsv',
    python_callable=extract_data_from_tsv_function,
    dag=dag,
)

# Function to consolidate data
def consolidate_data_function():
    csv_file = '/home/project/airflow/dags/finalassignment/unzipped_data/csv_data.csv'  
    tsv_file = '/home/project/airflow/dags/finalassignment/unzipped_data/tsv_data.csv'  
    fixed_width_file = '/home/project/airflow/dags/finalassignment/unzipped_data/fixed_width_data.csv'  
    output_file = '/home/project/airflow/dags/finalassignment/unzipped_data/extracted_data.csv'

    with open(csv_file, 'r') as csv_infile, open(tsv_file, 'r') as tsv_infile, open(fixed_width_file, 'r') as fixed_infile, open(output_file, 'w', newline='') as outfile:
        csv_reader = csv.DictReader(csv_infile)
        tsv_reader = csv.DictReader(tsv_infile)
        fixed_reader = csv.DictReader(fixed_infile, delimiter='|')  

        fieldnames = [
            'Rowid', 'Timestamp', 'Anonymized Vehicle number', 'Vehicle type',
            'Number of axles', 'Tollplaza id', 'Tollplaza code',
            'Type of Payment code', 'Vehicle Code'
        ]
        writer = csv.DictWriter(outfile, fieldnames=fieldnames)

        writer.writeheader()
        for csv_row, tsv_row, fixed_row in zip(csv_reader, tsv_reader, fixed_reader):
            consolidated_row = {
                'Rowid': csv_row['Rowid'],
                'Timestamp': csv_row['Timestamp'],
                'Anonymized Vehicle number': csv_row['Anonymized Vehicle number'],
                'Vehicle type': csv_row['Vehicle type'],
                'Number of axles': tsv_row['Number of axles'],
                'Tollplaza id': tsv_row['Tollplaza id'],
                'Tollplaza code': tsv_row['Tollplaza code'],
                'Type of Payment code': fixed_row['Type of Payment code'],
                'Vehicle Code': fixed_row['Vehicle Code'],
            }
            writer.writerow(consolidated_row)


# Task to consolidate data
consolidate_data = PythonOperator(
    task_id='consolidate_data',
    python_callable=consolidate_data_function,
    dag=dag,
)

transform_data = BashOperator(
    task_id='transform_data',
    bash_command="awk -F',' '{OFS=\",\"; $4=toupper($4)} 1' /home/project/airflow/dags/finalassignment/unzipped_data/extracted_data.csv > /home/project/airflow/dags/finalassignment/unzipped_data/transformed_data.csv",
    dag=dag,
)


# Define task pipeline
unzip_data >> extract_data_from_csv >> extract_data_from_tsv >> consolidate_data >> transform_data




# To submit the DAG and ensure it runs properly:

# ### **Using CLI**:
# 1. **Place the DAG file**: 
#    - Copy `ETL_toll_data.py` to the Airflow DAGs directory. This is typically located at `/home/project/airflow/dags/`.
#    - Use the following command:
#      ```bash
#      cp /path/to/ETL_toll_data.py /home/project/airflow/dags/
#      ```

# 2. **Verify the DAG**:
#    - Restart the Airflow scheduler and webserver:
#      ```bash
#      airflow scheduler &
#      airflow webserver &
#      ```
#    - Check for the DAG in the Airflow web interface under the "DAGs" section.

# 3. **List available DAGs**:
#    - Run the following command to verify the DAG is recognized:
#      ```bash
#      airflow dags list
#      ```

# 4. **Trigger the DAG**:
#    - Trigger the DAG manually:
#      ```bash
#      airflow dags trigger ETL_toll_data
#      ```

# 5. **Check DAG status**:
#    - Monitor the DAG's execution in the Airflow web interface or by using:
#      ```bash
#      airflow tasks list ETL_toll_data
#      ```

# ---

# ### **Using Web UI**:
# 1. **Access Airflow Web Interface**:
#    - Open the web interface at `http://localhost:8080`.

# 2. **Locate the DAG**:
#    - Look for the `ETL_toll_data` DAG in the DAG list. Ensure it's in an "Active" state.

# 3. **Enable the DAG**:
#    - Toggle the switch to enable the DAG.

# 4. **Trigger DAG Execution**:
#    - Click on the "Trigger DAG" button (▶️) to execute the pipeline.

# 5. **Monitor the DAG**:
#    - Go to the "Graph View" or "Tree View" of the DAG to track task progress.
