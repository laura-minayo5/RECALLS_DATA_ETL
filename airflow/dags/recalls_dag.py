from airflow import DAG # To import the 'DAG' class from airflow
from datetime import datetime, timedelta # Importing datetime and timedelta modules/class for scheduling the DAGs
import airflow
# Importing operators 
from airflow.operators.python_operator import PythonOperator

import requests # Python Requests library for making HTTP requests to a retrieve our data from a specified URL
import pandas as pd # pandas library necessary for creating and maniplulating dataframes
import json
# importing sqlalchemy neccessary for establishing connection with postgresql db using a defined engine object
from sqlalchemy import create_engine 



# initializing default arguments to be used in our dag object
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# define PostgreSQL database connection credentials
db_config = {
    'dbname': 'recalls_db',
    'user': 'postgres',
    'password': 'secret',
    # Use the service name from docker-compose as the hostname
    'host': 'destination_postgres',
    'port': 5434
}

# define url for calling data from Canadian government data site
raw_url = 'https://recalls-rappels.canada.ca/sites/default/files/opendata-donneesouvertes/HCRSAMOpenData.json'

# data path to save raw data and cleaned data
raw_data_path = '/opt/airflow/data/raw/recalls_raw.csv'
cleaned_data_path = '/opt/airflow/data/cleaned/recalls_cleaned.csv'


# define the extraction function that takes API url
# and save the raw data as a csv

def get_recall_data(url, raw_output_path): # function takes two parameters, url and raw_output_path (op_kwargs)
    response = requests.get(url) # python requests using get method to retrieve info from the given server using a given url
    response_data = response.json() # returns a JSON object of the response returned from a request
    print(response)

    recall_list = [] # list that will store the retrieved data from the url once appended

    for r in response_data: # iterating over the response_data (JSON data) using for loop and converting it to Dict in Python
        recall = {
            'recall_id': r['NID'],
            # 'title': r['Title'],
            'organization': r['Organization'],
            'product_name': r['Product'],
            'issue': r['Issue'],
            'category': r['Category'],
            'updated_at': r['Last updated']
        }
        recall_list.append(recall) # adds the recall dict of lists values to the recall_list using append() methodd
    print(f"Added {len(recall_list)} recalled records.") # prints how many records have been added to the recall_list
    
    recall_df = pd.DataFrame(recall_list) # Creates a dataframe using recall_list and stores it in recall_df variable

    current_timestamp = datetime.now() # current_timestamp stores current time 

    recall_df['data_received_at'] = current_timestamp # creating and accessing new column data_received_at  from the df
                                                    # this column will store the current time data is being recieved into the df

    recall_df.to_csv(raw_output_path, index=False, header=True) # Exports recall_df (excluding the index col) 
                                                                # into raw_output_path (op_kwargs) csv

# define a transformation function and save the cleaned data into another csv
def clean_recall_data(raw_input_path, cleaned_output_path):
    df = pd.read_csv(raw_input_path) # reading raw_output_path csv file and storing it in df var
     # splits the category column in our df into two columns, issue_category and category(keeping this old column)
    df[['issue_category', 'category']] = df['category'].str.split(' - ', n=1, expand=True)
    # changes the format of updated_at using todatetime() method
    df['updated_at'] = pd.to_datetime(df['updated_at'], utc=True, format='%d/%m/%Y')
    # sorts the column updated_at and stores it in df
    df = df.sort_values(by=['updated_at'], ascending=False)
    df.to_csv(cleaned_output_path, index=False) # Exports df (excluding the index col) 
                                                # into cleaned_output_path(op_kwargs) csv

    print(f"Success: cleaned {len(df)} recall records.") # prints how many clean records are in df

# define a function to load the data to postgresql database
# and use pd.sql() to load the transformed data to the database directly
def load_to_db(db_host, db_name, db_user, db_pswd, db_port, data_path): # (op_kwargs)
    df = pd.read_csv(data_path)
    current_timestamp = datetime.now()
    df['data_ingested_at'] = current_timestamp

    # establishing a connection with the postgresql database using sqlalchemy by defining conn. engine
    # engine = create_engine('postgresql+psycopg2://user:password@hostname/database_name') refer to op_kwargs for db details
    engine = create_engine(f"postgresql+psycopg2://{db_user}:{db_pswd}@{db_host}/{db_name}")

    # converting the cleaned dataframe df into an SQL database so we can query it later using sql
    # DataFrame.to_sql(name, con, *, schema=None, if_exists='fail', index=True, index_label=None, chunksize=None, dtype=None, method=None)
    df.to_sql('recalls', con=engine, schema='data', if_exists='replace', index=False)
    # printing how many recall records have been added to our database
    print(f"Success: Loaded {len(df)} recall records to {db_name}.")



# Creating DAG Object
dag = DAG(
    dag_id ='recalls_etl',
    default_args = default_args, # refers to the default_args dictionary
    description = 'An ETL workflow using pandas',
    start_date = datetime(2025, 1, 1), # datetime() class requires three parameters to create a date: year, month, day
    schedule_interval = timedelta(days=1), # takes cron(* * * * * ) or timedelta(days=0, seconds=0, microseconds=0, milliseconds=0, minutes=0, hours=0, weeks=0) values
    catchup = False # tells scheduler not to backfill all missed DAG runs between the current date and the start date when the DAG is unpaused.
)

# defining task using Airflow PythonOperator for raw data extraction
get_raw_data = PythonOperator(
    task_id='get_raw_data',
    python_callable=get_recall_data, # A reference to get_recall_data function that is callable
    # a dictionary of keyword arguments that will get unpacked in your function when referenced.
    op_kwargs={
        'url': raw_url,  # refer to raw_url and raw_data_path at the begining of the code
        'raw_output_path': raw_data_path
    },
    dag=dag,
)

# defining a task using Airflow PythonOperator for cleaning raw data
clean_raw_data = PythonOperator(
    task_id='clean_raw_data',
    python_callable=clean_recall_data, # A reference to clean_recall_data function that is callable
    # a dictionary of keyword arguments that will get unpacked in your function when referenced.
    op_kwargs={
        'raw_input_path': raw_data_path, # refers to raw_data_path at the  beginning of code
        'cleaned_output_path': cleaned_data_path # refers to cleaned_data_path at the beginning of code
    },
    dag=dag,
)

# defining a task using Airflow PythonOperator to load cleaned data to PostgreSQL database
load_data_to_db = PythonOperator(
    task_id='load_data_to_db',
    python_callable=load_to_db,
     # a dictionary of keyword arguments that will get unpacked in your function when referenced.
     op_kwargs={
        'db_host':db_config['host'], # refer to db_config dict at the beginning of code
        'db_name':db_config['dbname'],
        'db_user':db_config['user'],
        'db_pswd':db_config['password'],
        'db_port':db_config['port'],
        'data_path': cleaned_data_path # refers to cleaned_data_path at the beginning of code
    
    },
    dag=dag,
)

# set the order to execute each of the tasks in the DAG
get_raw_data >> clean_raw_data >> load_data_to_db