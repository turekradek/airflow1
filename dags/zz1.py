from datetime import timedelta
from datetime import datetime
from genericpath import isfile 
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
    schedule_interval='*/2 * * * *',
    catchup=False,
    dagrun_timeout=timedelta(minutes=60),
)


def make_files():
    my_path=r'/home/rturek2/in'
    ile = random.randint(3,10)
    now = datetime.now()
    for i in range( ile ):
        date_today = now.strftime("%d_%m_%Y_%H__%M_%S_")
        nazwa = [ chr(random.randint(97,122))  for i in range(random.randint(2,4))]
        rozszerzenie = '.txt'
        nazwa = ''.join([date_today,''.join(nazwa),rozszerzenie])
        nazwa = os.path.join(my_path,nazwa)
        print( nazwa )
        with open(nazwa , 'w') as f:
            f.write(f'plik o nazwie ')
    return 'cos sie stalo'
        

def move_files():
    my_path=r'/home/rturek2/in'
    destination = r'/home/rturek2/out'
    lista = os.listdir(my_path)
    files = [   el  for el in lista if os.path.isfile   ]
    # directories = [  os.path.join(destination,el)  for el in lista if os.path.isdir   ]
    for el in files:
        shutil.move( os.path.join(my_path,el), os.path.join(destination, el) )
        print( el )
        
    return 'ZROBIONE'



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
    