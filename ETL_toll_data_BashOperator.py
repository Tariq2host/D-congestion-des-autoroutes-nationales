from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash_operator import BashOperator

# Define default arguments for the DAG
default_args = {
    'owner': 'dummy_name',  # Replace with any dummy name
    'start_date': datetime.now(),
    'email': ['dummy_email@example.com'],  # Replace with any dummy email
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Initialize the DAG
dag = DAG(
    'ETL_toll_data_bash',
    default_args=default_args,
    description='A simple ETL DAG for toll data using BashOperator',
    schedule_interval=timedelta(days=1),
)

# Task to unzip data
unzip_data = BashOperator(
    task_id='unzip_data',
    bash_command="""
    mkdir -p /home/project/airflow/dags/finalassignment/unzipped_data && \
    tar -xzf /home/project/airflow/dags/finalassignment/tolldata.tgz -C /home/project/airflow/dags/finalassignment/unzipped_data
    """,
    dag=dag,
)

# Task to extract specific fields from CSV
extract_data_from_csv = BashOperator(
    task_id='extract_data_from_csv',
    bash_command="""
    awk -F',' 'BEGIN {OFS=","} NR==1 {print "Rowid,Timestamp,Anonymized Vehicle number,Vehicle type"} \
    NR>1 {print $1, $2, $3, $4}' \
    /home/project/airflow/dags/finalassignment/unzipped_data/vehicle-data.csv > \
    /home/project/airflow/dags/finalassignment/unzipped_data/csv_data.csv
    """,
    dag=dag,
)

# Task to extract specific fields from TSV
extract_data_from_tsv = BashOperator(
    task_id='extract_data_from_tsv',
    bash_command="""
    awk -F'\t' 'BEGIN {OFS=","} NR==1 {print "Number of axles,Tollplaza id,Tollplaza code"} \
    NR>1 {print $1, $2, $3}' \
    /home/project/airflow/dags/finalassignment/unzipped_data/tollplaza-data.tsv > \
    /home/project/airflow/dags/finalassignment/unzipped_data/tsv_data.csv
    """,
    dag=dag,
)

# Task to consolidate data
consolidate_data = BashOperator(
    task_id='consolidate_data',
    bash_command="""
    paste -d',' \
    /home/project/airflow/dags/finalassignment/unzipped_data/csv_data.csv \
    /home/project/airflow/dags/finalassignment/unzipped_data/tsv_data.csv \
    /home/project/airflow/dags/finalassignment/unzipped_data/fixed_width_data.csv | \
    awk -F',' 'BEGIN {OFS=","} NR==1 {print "Rowid,Timestamp,Anonymized Vehicle number,Vehicle type,Number of axles,Tollplaza id,Tollplaza code,Type of Payment code,Vehicle Code"} \
    NR>1 {print $1, $2, $3, $4, $5, $6, $7, $8, $9}' > \
    /home/project/airflow/dags/finalassignment/unzipped_data/extracted_data.csv
    """,
    dag=dag,
)

# Task to transform data
transform_data = BashOperator(
    task_id='transform_data',
    bash_command="""
    awk -F',' 'BEGIN {OFS=","} {if (NR==1) print; else { $4=toupper($4); print }}' \
    /home/project/airflow/dags/finalassignment/unzipped_data/extracted_data.csv > \
    /home/project/airflow/dags/finalassignment/unzipped_data/transformed_data.csv
    """,
    dag=dag,
)

# Define task pipeline
unzip_data >> extract_data_from_csv >> extract_data_from_tsv >> consolidate_data >> transform_data
