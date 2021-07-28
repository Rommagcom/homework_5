import os
from datetime import datetime
from requests import HTTPError
from datetime import datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import psycopg2
from psycopg2.extras import RealDictCursor
from contextlib import closing
from hdfs import InsecureClient

def uploadFromDb(**kwargs):

    client = InsecureClient('http://127.0.0.1:50070/', user='user')

    process_date = datetime.now()
    process_date_str = process_date.strftime("%d_%m_%Y")

    directory = os.path.join('bronze','dshop_db_tables',process_date_str)
    client.makedirs(directory) # Directory creation

    sources=[
        {"table":"aisles","output":"ailses.csv"},
        {"table":"clients","output":"clients.csv"},
        {"table":"departments","output":"departments.csv"},
        {"table":"orders","output":"orders.csv"},
        {"table":"products","output":"products.csv"}]
    
    with closing(psycopg2.connect(dbname='dshop', user='pguser', password='secret', host='192.168.1.230')) as conn:
        for source in sources:
            with conn.cursor() as cursor:
                with client.write(os.path.join(directory, source['output']),overwrite=True) as csv_file:
                    cursor.copy_expert('COPY (SELECT * FROM '+source['table']+') TO STDOUT WITH HEADER CSV', csv_file)


dag = DAG(
    dag_id = 'import_data_to_hdfs_from_DB',
    description = 'Save data from DB and store to HDFS',
    start_date = datetime(2021, 7, 29, 23, 59),
    schedule_interval='@daily'
)

t1 = PythonOperator(
    task_id='upload_db_to_csv',
    dag=dag,
    python_callable=uploadFromDb,
    provide_context=True
)
                    