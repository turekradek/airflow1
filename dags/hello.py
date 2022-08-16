from datetime import timedelta

import airflow
from airflow.models import DAG 
from airflow.operators.python_operator import PythonOperator 
from airflow.operators.bash_operator import BashOperator 
from airflow.operators.dummy_operator import DummyOperator 

args ={
    'owner':'airflow',
    'start_date':airflow.utils.dates.days_ago(1),
}

dag = DAG(
    dag_id='hello_world',
    default_args=args,
    schedule_interval='*/2 * * * *',
    catchup=False,
    dagrun_timeout=timedelta(minutes=60),
)

t_dummy = DummyOperator(
    task_id='dummy',
    dag=dag,
)

t_bash_hello = BashOperator(
    task_id='bash_hello',
    bash_command='echo "Hello i am BashOperator ',
    dag=dag,
)

def print_hello_world(recipient):
    message='Hello, {} from PythonOperator'.format(recipient)
    print( message)

    # printed to logs 
    return message 

t_python_hello = PythonOperator(
    task_id='python_hello',
    python_callable=print_hello_world,
    dag=dag,
)
# zaleznosci taskow
t_dummy >> t_bash_hello
t_dummy >> t_python_hello