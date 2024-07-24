from minio import Minio
import os
from dotenv import load_dotenv
from datetime import datetime
import urllib3
import logging
import os.path
from dotenv import load_dotenv 
from batch_unstructured.py.google_api import *
from batch_unstructured.py.queries import *

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
