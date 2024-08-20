from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python_operator import PythonOperator


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2024, 8, 19, 8, 45, 0),
}

dag = DAG(
    'pascal',
    default_args=default_args,
    description='Pascal triangle with 10 levels in Python',
    schedule_interval='@daily',
    catchup=False,
)

def printPascal(n):

    for line in range(0, n):

        for i in range(0, line + 1):
            print(binomialCoeff(line, i),
                  " ", end="")
        print()

def binomialCoeff(n, k):
    res = 1
    if (k > n - k):
        k = n - k
    for i in range(0, k):
        res = res * (n - i)
        res = res // (i + 1)

    return res

n = 10

pascal = PythonOperator(
    task_id='print_pascal',
    python_callable=printPascal(n),
    dag=dag,
)

pascal
