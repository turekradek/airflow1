from asyncore import file_wrapper
from datetime import timedelta, datetime
import pandas as pd 
import os.path 

import airflow 
from airflow.models import DAG 
from airflow.operators.python_operator import PythonOperator, ShortCircuitOperator

args = {
    'owner':'airflow',
    'start_date':airflow.utils.dates.days_ago(1),

}

dag = DAG(
    dag_id = 'temperature_etl',
    default_args=args,
    schedule_interval='@daily',
    catchup=True,
)

EXTRACT_PATH='/home/rturek2/in'
LOAD_PATH='/home/rturek2/out'
file_prefix = (datetime.utcnow()-timedelta(days=1)).strftime('%Y-%m-%d')

def check_for_temperature(**kwargs):
    print('check_for_temperature')
    input_file = ''.join([kwargs.get('scr_path'),kwargs.get('file_prefix'),'_temperatures.csv'])
    print( input_file)
    found_csv = os.path.isfile(input_file)
    return found_csv

t_check = ShortCircuitOperator(
    python_callable=check_for_temperature,
    task_id='check_for_temperature',
    op_kwargs={'src_path': EXTRACT_PATH,
            'file_prefix':file_prefix},
    dag=dag,
    
)

def temperature_csv_to_json(**kwargs):
    print('temperature_csv_to_json')
    input_file = ''.join([kwargs.get('scr_path'),kwargs.get('file_prefix'),'_temperatures.csv'])
    output_file = ''.join([kwargs.get('dest_path'),kwargs.get('file_prefix'),'_temperatures.json'])
    print( input_file)
    print( output_file)
    df = pd.read_csv(input_file)
    df.to_json(output_file,orient='records')
    return output_file

t_transform = PythonOperator(
    task_id='temperature_to_json',
    python_callable=temperature_csv_to_json,
    op_kwargs={'src_path':EXTRACT_PATH,
        'dest_path': LOAD_PATH,
        'file_prefix':file_prefix},
        dag=dag,

)