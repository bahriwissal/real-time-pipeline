from datetime import datetime 
from airflow import DAG 
from airflow.operators.python import PythonOperator

import json 
import requests
import time 
import logging

from kafka import KafkaProducer 

default_args = {
    'owner' : 'jase',
    'start_date' : datetime(2024, 11, 1, 10, 00)
}

# Function to get data from the api 
def get_data():
    response = requests.get("https://randomuser.me/api/")

    # we need only access to the results part, and [0] is to get the result as a json not as a list 
    response = response.json()['results'][0]
    
    #print(json.dumps(response, indent=3))

    return response

# The format of the data that i want to put on my kafka queue
# I need to extract the data i have and then format it as i want 
def format_data(response):
    data = {}
    data['gender'] = response['gender']
    data['first_name'] = response['name']['first']
    data['last_name'] = response['name']['last']

    location = response['location']
    data['address'] = f"{str(location['street']['number'])} {location['street']['name']}, " \
                      f"{location['city']}, {location['state']}, {location['country']}"
    data['post_code'] = location['postcode']
    data['email'] = response['email']
    data['username'] = response['login']['username']
    data['dob'] = response['dob']['date']
    data['registered_date'] = response['registered']['date']
    data['phone'] = response['phone']
    data['picture'] = response['picture']['large']

    return data

# define the function stream data 
def stream_data():

    # Kafka producer will helps us to send data to kafka.
    # It acts as teh data source, generating messages and sending them to Kafka brokers,
    # where they are stored and can be consumed by Kafka Consumers 

    # If it is running on local : 'localhost:9092'
    # If it is running on docker instance, it should be 'broker:29092'
    producer = KafkaProducer(bootstrap_servers=['broker:29092'], max_block_ms=5000)

    current_time = time.time()
    logging.info(f'time is {current_time}')

    while True:
        if time.time() > current_time + 60 : #1 min
            break
        try : 
            response = get_data()
            data = format_data(response)
            producer.send('users_created', json.dumps(data).encode('utf-8'))
        except Exception as e : 
            logging.error(f'An error occured : {e}')
            continue
            

# Create a dag that gets data from the api 
with DAG('user_automation',
         default_args=default_args,
         schedule_interval='@daily',
         catchup=False) as dag:

    streaming_task = PythonOperator(
        task_id='stream_data_from_api',
        python_callable=stream_data
    )

stream_data()