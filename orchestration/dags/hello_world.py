from airflow import DAG
from airflow.operators.bash import BashOperator
# from airflow.providers.standard.operators.bash import BashOperator
from datetime import datetime
# from airflow.utils.dates import days_ago

args = {
    'owner': 'abdul-developer',
}

with DAG(
    dag_id = 'hello_world_airflow',
    default_args=args,
    schedule='0 5 * * *',
    start_date=datetime(2026, 1, 1),
) as dag:
    
    print_hello = BashOperator(
        task_id = 'print_hello',
        bash_command = 'echo Hello',
    )

    print_world = BashOperator(
        task_id = 'print_world',
        bash_command = 'echo World',
    )

    print_hello >> print_world

if __name__ == "__main__":
    dag.cli()

