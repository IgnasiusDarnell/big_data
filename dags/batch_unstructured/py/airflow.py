from minio import Minio
import os
from dotenv import load_dotenv
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import urllib3
import logging
import os.path
from dotenv import load_dotenv 
from batch_unstructured.py.google_api import *
from batch_unstructured.py.queries import *
from batch_unstructured.py.minio_api import *
from batch_unstructured.py.process_csv_files import *

# Define the SCOPES. If modifying it, 
# delete the token.pickle file. 
load_dotenv()

default_args = {
			'owner': 'airflow',
			'start_date': datetime(2023, 9, 3, 10, 00)
		}

class batch_DAG(googleDriveAPI):
	def __init__(self):
		super().__init__()
		self.server_name = os.getenv('MINIO_LOCAL_SERVERNAME')
		self.access_key = os.getenv('MINIO_LOCAL_ACCESS_KEY')
		self.password = os.getenv('MINIO_LOCAL_PASSWORD')
		self.bucket_name = os.getenv("MINIO_BUCKET")
		self.is_bucket_found = False
		self.minio_client = None
		self.file = None

	def connect_to_minio(self):
		logging.info("Initiate Connection to Minio: ")
		logging.info("Server Name: "+self.server_name)

		self.minio_client = Minio(
			self.server_name,
			access_key=self.access_key,
			secret_key=self.password,
			secure=False,
			http_client=urllib3.PoolManager(
			num_pools=10,
			)
		)

	def check_bucket_exist(self):
		# Make the bucket if it doesn't exist.
		self.is_bucket_found = self.minio_client.bucket_exists(self.bucket_name)
		logging.info(self.is_bucket_found)
	
	def upload_file(self):
		self.minio_client.put_object(self.bucket_name,
									 data=self.file,
									 object_name="{}/{}".format(self.file_data['folder_name'],self.file_data['name']),
									 length=self.file.getbuffer().nbytes)

	# Upload the file, renaming it in the process
	def send_data_to_bucket_by_name(self):
		self.minio_client.fput_object(
			self.bucket_name, self.destination_file, self.source_file,
		)
		print(
			self.source_file, "successfully uploaded as object",
			self.destination_file, "to bucket", self.bucket_name,
		)

	def download_and_produce(self):
		self.download_file()
		self.upload_file()
		# data.delete_file()

def dag_operate():
	import os
	import json
	from kafka import KafkaProducer
	producer = KafkaProducer(bootstrap_servers=['broker-1:29093','broker:29092'])
	data = batch_DAG()

	data.connect_to_minio()
	data.connect_with_service_account()
	data.check_bucket_exist()
	if(not data.is_bucket_found):
		data.minio_client.make_bucket(os.getenv("MINIO_BUCKET"))

	data.iterate_and_get_files()
	postgre_connection = PropertiesConsumer()
	postgre_connection.connect_to_postgre()
	postgre_connection.create_session()
	for message in data.file_lists:
		postgre_connection.data = message
		postgre_connection.filter_object_by_name()
		temp = postgre_connection.data
		if(postgre_connection.selected_object.count()>0):
			temp = postgre_connection.selected_object.first()
			if(not(temp.size == postgre_connection.data['size'] and temp.modified_time == postgre_connection.data['modifiedTime'])):
				data.file_data = message
				data.download_file()
				data.upload_file()
				print("SENDING NEW DATA: ",message)
				producer.send('batch_upload_properties', json.dumps(message).encode('utf-8'))
		
		elif(postgre_connection.selected_object.count()==0):
			data.file_data = message
			data.download_file()
			data.upload_file()
			print("SENDING NEW DATA: ",message)
			producer.send('batch_upload_properties', json.dumps(message).encode('utf-8'))
	producer.flush()
	producer.close()
	# except Exception as e:
	# 	logging.info("Error occured. ",e)


with DAG('bulk_uploading_dag',
		 default_args=default_args,
		 schedule_interval='@daily',
		 catchup=False) as dag:

	bulk_upload_from_gdrive = PythonOperator(
		task_id='bulk_upload_from_gdrive',
		python_callable=dag_operate,
	)

	nakes_categorical_postgre_task = PythonOperator(
		task_id='nakes_csv_to_postgre',
		python_callable=generalProcessCSVFile().operate,
	)
	nakes_registered_postgre_task = PythonOperator(
		task_id='nakes_registered_csv_to_postgre',
		python_callable=RegisteredNakesProcessCSVFile().operate,
	)
	properties_consumer_task = PythonOperator(
		task_id='properties_consume_task',
		python_callable=PropertiesConsumer().consume_and_insert,
	)

	bulk_upload_from_gdrive >> [nakes_categorical_postgre_task,nakes_registered_postgre_task,properties_consumer_task]
