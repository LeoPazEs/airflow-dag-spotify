from datetime import datetime, timedelta

# OBJETO DAG
from airflow import DAG  

#Operator to run python in DAG
from airflow.operators.python_operator import PythonOperator 

default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
        # 'queue': 'bash_queue',
        # 'pool': 'backfill',
        # 'priority_weight': 10,
        # 'end_date': datetime(2016, 1, 1),
        # 'wait_for_downstream': False,
        # 'sla': timedelta(hours=2),
        # 'execution_timeout': timedelta(seconds=300),
        # 'on_failure_callback': some_function,
        # 'on_success_callback': some_other_function,
        # 'on_retry_callback': another_function,
        # 'sla_miss_callback': yet_another_function,
        # 'trigger_rule': 'all_success'
    } 

dag = DAG( 
    "testing_dag", 
    default_args=default_args, 
    description=" Dag created for the interview in MESHA TECNOLOGIA.",
    start_date = datetime(2022, 4, 16),
    schedule_interval=timedelta(days=1),
    #CATCHUP -> by default, will kick off a DAG Run for any data interval that has not been run since the last data interval (or has been cleared)
    catchup=False,
    tags=["interview"],
) 

def testing_dag(): 
    print("I am working :D!")

run = PythonOperator(
    task_id = "testing",
    python_callable= testing_dag,
    dag = dag
) 

run