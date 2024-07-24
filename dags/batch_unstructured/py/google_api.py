# import the required libraries 
import pickle 
import os.path 
from googleapiclient.discovery import build 
from google_auth_oauthlib.flow import InstalledAppFlow 
from google.auth.transport.requests import Request 
from dotenv import load_dotenv
import io
from googleapiclient.errors import HttpError
from googleapiclient.http import MediaIoBaseDownload
import tempfile
from oauth2client.service_account import ServiceAccountCredentials
from googleapiclient.http import MediaFileUpload

# Define the SCOPES. If modifying it, 
# delete the token.pickle file. 
load_dotenv()

class googleDriveAPI():
	def __init__(self):
		self.N = 0
		# self.path = Variable.get("path")
		self.path = "dags/batch_unstructured/creds/"
		self.cred_file_name =os.getenv("CRED_FILE_NAME")
		self.token_file_name = os.getenv("TOKEN_FILE_NAME")
		self.SCOPES = ['https://www.googleapis.com/auth/drive']
		# Variable creds will store the user access token. 
		# If no valid token found, we will create one. 
		self.creds = None
		self.file_lists = []
		self.folder_lists = []
		self.file_data = []

	def connect_with_service_account(self):
		# Load the json format key that you downloaded from the Google API
		# Console when you created your service account. For p12 keys, use the
		# from_p12_keyfile method of ServiceAccountCredentials and specify the
		# service account email address, p12 keyfile, and scopes.

		scope = ['https://www.googleapis.com/auth/drive']
		self.credentials = ServiceAccountCredentials.from_json_keyfile_name(
            self.path + self.cred_file_name, scopes=scope)
		# Create an httplib2.Http object to handle our HTTP requests and authorize
		# it with the Credentials.

		self.service = build('drive', 'v3', credentials=self.credentials)

		# Call the Drive v3 API
		# results = self.service.files().list(pageSize=1000, fields="nextPageToken, files(id, name, mimeType, size, modifiedTime)").execute()
		# # get the results
		# items = results.get('files', [])
		# print(results)

	# Create a function getFileList with 
	# parameter N which is the length of 
	# the list of files. 
	def connect(self): 
		# The file token.pickle stores the 
		# user's access and refresh tokens. It is 
		# created automatically when the authorization 
		# flow completes for the first time. 

		# Check if file token.pickle exists 
		if os.path.exists(self.token_file_name): 

			# Read the token from the file and 
			# store it in the variable creds 
			with open(self.path + self.token_file_name, 'rb') as token: 
				self.creds = pickle.load(token) 

		# If no valid credentials are available, 
		# request the user to log in. 
		if not self.creds or not self.creds.valid: 

			# If token is expired, it will be refreshed, 
			# else, we will request a new one. 
			if self.creds and self.creds.expired and self.creds.refresh_token: 
				self.creds.refresh(Request()) 
			else: 
				self.flow = InstalledAppFlow.from_client_secrets_file( 
					self.cred_file_name, self.SCOPES) 
				self.creds = self.flow.run_local_server(port=0) 

			# Save the access token in token.pickle 
			# file for future usage 
			with open(self.path + self.token_file_name, 'wb') as token: 
				pickle.dump(self.creds, token) 

		# Connect to the API service 
		self.service = build('drive', 'v3', credentials=self.creds)

	def get_existing_folders(self,N=1000):
		# request a list of first N files or 
		# folders with name and id from the API. 
		resource = self.service.files() 
		result = resource.list(q = "mimeType='application/vnd.google-apps.folder'",pageSize=N, fields="files(id, name)").execute() 

		# return the result dictionary containing 
		# the information about the files 
		return result 
	
	def download_file_from_drive(self,file_id):
		try:
			# pylint: disable=maybe-no-member
			request = self.service.files().get_media(fileId=file_id)
			self.file = io.BytesIO()
			downloader = MediaIoBaseDownload(file, request)
			done = False
			while done is False:
				self.status, self.done = downloader.next_chunk()
				print(f"Download {int(self.status.progress() * 100)}.")

		except HttpError as error:
			print(f"An error occurred: {error}")
			file = None

		return file.getvalue()

	def list_of_files(self):
		"""Shows basic usage of the Drive v3 API.
		Prints the names and ids of the first 5 files the user has access to.
		"""
		# Call the Drive v3 API
		results = self.service.files().list(
			q = "mimeType!='application/vnd.google-apps.folder' and '"+self.folder_data['id']+"' in parents",
			fields="nextPageToken, files(id, name, mimeType, size, parents, modifiedTime)").execute()
		# get the results
		temp = results.get('files', [])

		for file in range(len(temp)):
			temp[file]['folderId'] = self.folder_data['id']
			temp[file]['folder_name'] = self.folder_data['name']

		if(temp!=[]):
			self.file_lists += temp

		print("File List: ",self.file_lists)
	
	def list_of_folders(self):
		"""Shows basic usage of the Drive v3 API.
		Prints the names and ids of the first 5 files the user has access to.
		"""
		self.folderId = os.getenv("GDRIVE_FOLDER_ID")
		# Call the Drive v3 API
		results = self.service.files().list(
			q = "mimeType='application/vnd.google-apps.folder' and '"+self.folderId+"' in parents",
			fields="nextPageToken, files(id, name, mimeType, size, parents, modifiedTime)").execute()
		# get the results
		self.folder_lists = results.get('files', [])
		self.number_of_folders = len(self.folder_lists)
		# print("Folder List: ",self.folder_lists)
	
	def download_file(self):
		try:
			request = self.service.files().get_media(fileId=self.file_data['id'])
			self.file = io.BytesIO()
			downloader = MediaIoBaseDownload(self.file, request)
			done = False
			while done is False:
				status, done = downloader.next_chunk()
				print(f"Download {int(status.progress() * 100)}.")
			self.file.seek(0)

		except HttpError as error:
			print(f"An error occurred: {error}")
			self.file = None
	
	def delete_file(self):
		self.service.files().delete(fileId=self.file_data['id']).execute()

	def check_if_folder_exists(self):
		results = self.service.files().list(
			q = "'"+self.folder_data['id']+"' in parents",
			fields="nextPageToken, files(id, name, mimeType, size, parents, modifiedTime)").execute()
		
		self.check_if_folder_exist_results = results.get('files', [])!=[]

	def create_folder_in_drive(self):
		folder_metadata = {
			"name": self.folder_data['name'],
			"mimeType": "application/vnd.google-apps.folder",
			"parents":self.folder_data['id']
			}
		self.service.files().create(body=folder_metadata,fields="id").execute()

	def upload_file_to_drive(self):
		file_metadata = {
        	"name": "test.txt",
        	"parents": [self.file_data['folder_id']]
    	}
		# upload
		media = MediaFileUpload("test.txt", resumable=True)
		file = self.service.files().create(body=file_metadata, media_body=media, fields='id').execute()
		print("File created, id:", file.get("id"))

	def create_historical_storage(self):
		self.list_of_folders()

		self.check_if_folder_exists()
		if(not self.check_if_folder_exist_results):
			self.create_folder()
			

	def iterate_and_get_files(self):
		self.list_of_folders()

		for folder in self.folder_lists:
			self.folder_data = folder
			self.list_of_files()