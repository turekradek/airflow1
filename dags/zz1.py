from datetime import timedelta
from datetime import datetime 
import airflow
from airflow.models import DAG 
from airflow.operators.python_operator import PythonOperator 
from airflow.operators.bash_operator import BashOperator 
from airflow.operators.dummy_operator import DummyOperator 

import os 
import shutil 
import random 

args ={
    'owner':'airflow',
    'start_date':airflow.utils.dates.days_ago(1),
}

dag = DAG(
    dag_id='zz1',
    default_args=args,
    schedule_interval='* */5 * * *',
    catchup=False,
    dagrun_timeout=timedelta(minutes=60),
)


def make_files(my_path=r'/home/radek/in'):
    ile = random.randint(3,10)
    now = datetime.now()
    for i in range( ile ):
        date_today = now.strftime("%d/%m/%Y, %H:%M:%S")
        nazwa = [ random.randint(97,122)  for i in random.randint(5,10)]
        rozszerzenie = '.txt'
        nazwa = [date_today,''.join(nazwa),rozszerzenie]
        with open(nazwa , 'w') as f:
            f.write(f'plik o nazwie {nazwa}')
        

def move_files(my_path=r'/home/radek/in'):
    destination = r'/home/radek/out'
    lista = os.listdir()
    files = [  el  for el in lista if os.path.isfile   ]
    directories = [  el  for el in lista if os.path.isdir   ]
    for el in files:
        shutil.move( os.path.join(my_path,el), os.path.join(destination,el))
    if len( os.listdir() ) == 0:
        return 'przekopiowane'



t_create_files = PythonOperator(
    task_id='create_files',
    python_callable=make_files,
    dag=dag,
)

t_move_files = PythonOperator(
    task_id='move_files',
    python_callable=move_files,
    dag=dag,
)

# zaleznosci taskow
t_create_files >> t_move_files
    