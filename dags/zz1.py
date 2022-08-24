from datetime import timedelta
from datetime import datetime
from genericpath import isfile 
from requests import get
import airflow
from airflow.models import DAG 
from airflow.operators.python_operator import PythonOperator 
from airflow.operators.bash_operator import BashOperator 
from airflow.operators.dummy_operator import DummyOperator 
from airflow.operators.http_operator import SimpleHttpOperator 
import pandas as pd 
import json
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
    schedule_interval='*/1 * * * *',
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


def informations(dag_id='zz1',default_return_value='now-12h'):
    endpoint_url = "http://localhost:8080/api/experimental/dags/"+dag_id+"/dag_runs"
    credentials={
        # "username": BaseHook.get_connection(username).login,
        # "password": BaseHook.get_connection(username).password,
        "username":"radek",
        "password":"radek"
    }
    credentials1={
        # "username": BaseHook.get_connection(username).login,
        # "password": BaseHook.get_connection(username).password,
        "username":"admin",
        "password":"admin"
    }
    credentials2={
        # "username": BaseHook.get_connection(username).login,
        # "password": BaseHook.get_connection(username).password,
        "username":"admin",
        "password":"airflow"
    }
    credentials3={
        # "username": BaseHook.get_connection(username).login,
        # "password": BaseHook.get_connection(username).password,
        "username":"airflow",
        "password":"airflow"
    }
    credentials4={
        # "username": BaseHook.get_connection(username).login,
        # "password": BaseHook.get_connection(username).password,
        "username":"",
        "password":""
    }

    response = get(endpoint_url)
    response_ = get(endpoint_url, auth=(credentials1.get('username'), credentials1.get('password')))
    response__ = get(endpoint_url, auth=(credentials2.get('username'), credentials2.get('password')))
    response3 = get(endpoint_url, auth=(credentials3.get('username'), credentials.get('password')))
    response4 = get(endpoint_url, auth=(credentials3.get('username'), credentials.get('password')))

    print( f'TO JEST credential {endpoint_url}')
    print( f'*** {response}')

    print( f'TO JEST credentia1 {endpoint_url}')
    print( f'*** {response_}')

    print( f'TO JEST credentia2 {endpoint_url}')
    print( f'*** {response__}')

    print( f'TO JEST credentia3 {endpoint_url}')
    print( f'*** {response3}')

    print( f'TO JEST credentia3 {endpoint_url}')
    print( f'*** {response4}')

    return response, response_, response__, response3, response4
    # if response.status_code == 200:
        # print( f'RESPONSE OK  {response.status_code}')
    # dag_runs_df = pd.DataFrame(json.loads(response.text))
    # dag_runs_df = dag_runs_df[dag_runs_df.state=='SUCCESS']#FAILD

    # if dag_runs_df.empty:
    #     print('No previous failed runs, default: ' + default_return_value)
    #     return default_return_value
    # else:
    #     last_failed_run = dag_runs_df.iloc[-1:]
    #     start_date = last_failed_run['start_date'].values[0]
    #     start_date=start_date[0:19]
    #     print('Last failed run date: '+start_date)
    #     return start_date

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

t_informations = PythonOperator(
    task_id='informations',
    python_callable=informations,
    dag=dag,
)

t_simple_bash = BashOperator(
    task_id='check_something',
    bash_command='airflow list_tasks zz1',
    dag=dag
)
t_2_bash = BashOperator(
    task_id='check_example_bash_operator',
    bash_command='airflow list_tasks example_bash_operator',
    dag=dag
)

# task_get_posts  = SimpleHttpOperator(
#     task_id="get_posts",
#     http_conn_id="apli_posts",
#     endpoint='posts/',
#     method='GET',
#     response_filter=lambda response: json.loads(response.text),
#     log_response=True
# )

# zaleznosci taskow
t_create_files >> t_move_files
t_informations
t_simple_bash >> t_2_bash
# t_2_bash
# task_get_posts 