from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
import pandas as pd
import json
import time
import sys
import os

STREAMING_DATA_DIR = 'lakehouse-3/streaming-data'
MODEL_SCRIPT = 'MODELLING/model.py'
FRONTEND_APP = 'frontend/app.py'

def send_cumulative_data():
    """Read and send cumulative data batches"""
    # Read full dataset
    df = pd.read_csv(f"{STREAMING_DATA_DIR}/Bank-full.csv")
    
    # Load or create tracking info
    tracker_file = f"{STREAMING_DATA_DIR}/cumulative_tracker.json"
    if os.path.exists(tracker_file):
        with open(tracker_file, 'r') as f:
            tracker = json.load(f)
    else:
        tracker = {
            'hours_passed': 0,
            'total_records_sent': 0
        }
    
    # Calculate how many records to send this hour
    hours_passed = tracker['hours_passed'] + 1
    records_to_send = 500 * hours_passed  # 500 per hour cumulative
    
    if records_to_send > len(df):
        # Reset if we've gone through all data
        hours_passed = 1
        records_to_send = 500
        tracker['total_records_sent'] = 0
    
    # Get cumulative data
    cumulative_df = df.iloc[:records_to_send]
    
    # Save current batch for producer
    cumulative_df.to_csv(f"{STREAMING_DATA_DIR}/current_batch.csv", index=False)
    
    # Update tracker
    tracker.update({
        'hours_passed': hours_passed,
        'total_records_sent': records_to_send
    })
    
    # Save tracker
    with open(tracker_file, 'w') as f:
        json.dump(tracker, f)
    
    # Run producer script
    os.system(f"python {STREAMING_DATA_DIR}/prod-pict.py")
    
    print(f"Sent {records_to_send} records (Cumulative for {hours_passed} hours)")

# DAG arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 12, 17),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Create DAG
dag = DAG(
    'data_lakehouse_cumulative_pipeline',
    default_args=default_args,
    description='Automated pipeline for cumulative data sending',
     schedule='0 * * * *',  # Run every hour
    catchup=False
)

# Task 1: Send cumulative data
send_data = PythonOperator(
    task_id='send_cumulative_data',
    python_callable=send_cumulative_data,
    dag=dag
)

# Task 2: Run modeling script
run_model = BashOperator(
    task_id='run_model',
    bash_command=f'python {MODEL_SCRIPT}',
    dag=dag
)

# Task 3: Run frontend app
run_frontend = BashOperator(
    task_id='run_frontend',
    bash_command=f'''
    pkill -f "python {FRONTEND_APP}" || true
    python {FRONTEND_APP} &
    ''',
    dag=dag
)

# Set task dependencies
send_data >> run_model >> run_frontend