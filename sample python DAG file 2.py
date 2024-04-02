from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

default_args = {
  'owner': 'airflow',
}

with DAG(
    dag_id='hello_world_dag',
    default_args=default_args,
    schedule_interval=timedelta(days=1), 
    start_date=datetime(2024, 3, 27),
    catchup=False 
) as dag:

  def print_hello():
    print("Hello World!")

  print_hello_task = PythonOperator(
    task_id='print_hello',
    python_callable=print_hello
  )

  bash_greeting_task = BashOperator(
    task_id='bash_greeting',
    bash_command='echo "Hello Ajay, I\'m a Bash job"' 
  )

  # Adjust task dependencies
  print_hello_task >> bash_greeting_task 
