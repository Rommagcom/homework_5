import requests
import json
import os
from datetime import datetime
from requests import HTTPError
from datetime import datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from contextlib import closing
from hdfs import InsecureClient

def uploadFromApi(**kwargs): 
    process_dates = ['2021-01-02']

    directory = os.path.join('bronze','api_data','out_of_stock')
    client = InsecureClient('http://127.0.0.1:50070/', user='user')

    url = 'https://robot-dreams-de-api.herokuapp.com/auth'
    my_headers = {'Content-Type' : 'application/json'}
    params= {"username": "rd_dreams", "password": "djT6LasE"}

    json_data = json.dumps(params)
    try:
        response = requests.post(url, data=json_data,headers=my_headers)

        resp_auth = response.json()

        if response.status_code == 200: #If request is success getting order details
            for process_date in process_dates:
                client.makedirs(os.path.join(directory, process_date)) # Directory creation
                url = 'https://robot-dreams-de-api.herokuapp.com/out_of_stock'
                my_headers = {'Authorization' :'JWT ' + resp_auth['access_token']}
                params= {'date': process_date }
                response = requests.get(url, params=params, headers = my_headers)
                product_ids = list(response.json())
                for product_id in product_ids:
                    client.write(os.path.join(directory,process_date, str(product_id['product_id'])+'.json'), data=json.dumps(product_id), encoding='utf-8',overwrite=True)
               
    except requests.HTTPError:
        print('Error!')      

dag = DAG(
    dag_id = 'import_data_to_hdfs_from_api',
    description = 'Save data from API and store to HDFS',
    start_date = datetime(2021, 7, 29, 23, 59),
    schedule_interval='@daily'
)

t1 = PythonOperator(
    task_id='upload_from_api',
    dag=dag,
    python_callable=uploadFromApi,
    provide_context=True
)
                    