from sqlalchemy.orm import sessionmaker,scoped_session
from datetime import datetime
import sys
# sys.path.append("E:\\Exploration\\python\\freelance\\single-kafka\\big-data-freelance\\dags\\jogja_api")
from jogja_api.kafka_consume_stream import *
from sqlalchemy import create_engine, insert, text,exists,select
from batch_unstructured.py.models import *

class PropertiesConsumer(RandomAPIConsumer):
	def __init__(self):
		super().__init__()
		self.topic_id = "batch_upload_properties"
		
	def create_session(self):
		self.session = sessionmaker(bind=self.engine)
		self.scoped_sess = scoped_session(self.session)
		self.started_session = self.session()

	def filter_object_by_name(self):
		# self.is_object_exists = self.started_session.query(google_drive_properties_db).filter(exists(select(1).from_(goo))).exists()
		self.selected_object = self.started_session.query(google_drive_properties_db).filter(google_drive_properties_db.file_name == self.data['name'],).order_by(google_drive_properties_db.id)

	def add_new_record(self):
		new_data = google_drive_properties_db(file_name = self.data['name'],
										fileid = self.data['id'],
										folderid = self.data['folderId'],
										folder_name = self.data['folder_name'],
										modified_time = self.data['modifiedTime'],
										mimetype = self.data['mimeType'],
										size = self.data['size'])
		
		self.started_session.add(new_data)
		self.started_session.commit()

	def update_object(self):
		self.filter_object_by_name()
		if(self.selected_object.count()>0):
			temp = self.selected_object.first()
			if(temp.size != self.data['size'] or temp.modified_time != self.data['modifiedTime']):
				temp.file_name = self.data['name']
				temp.fileid = self.data['id']
				temp.folder_name = self.data['folder_name']
				temp.folderid = self.data['folderId']
				temp.modified_time = self.data['modifiedTime']
				temp.mimetype = self.data['mimeType']
				temp.size = self.data['size']
				temp.last_refresh_date = self.today_date
				self.started_session.commit()

	def upsert_object(self):
		self.update_object()
		if(self.selected_object.count()==0):
			print("ada data baru")
			self.add_new_record()
		
	def get_query(self):
		self.query=text("""INSERT INTO storage_properties_db
						(
						folderid,
						folder_name,
						fileid,
						file_name,
						mimetype,
						modified_time,
						size,
				  		last_refresh_date) VALUES
						(
						'{}',
						'{}',
						'{}',
						'{}',
						'{}',
						'{}',
						'{}',
						'{}')
						""".format(
									str(self.data['folderId']),
									str(self.data['folder_name']),
									str(self.data['id']),
									str(self.data['name']),
									str(self.data['mimeType']),
									datetime.strptime(self.data['modifiedTime'],"%Y-%m-%dT%H:%M:%S.%fZ").date(),
									float(self.data['size']),
									self.today_date))
	
	def delete_removed_record(self):
		self.query = self.started_session.query(google_drive_properties_db).filter(
			google_drive_properties_db.file_name.in_([json.loads(i.value)['name'] for i in self.consume_task])
		)
		
		# printing the data
		for data in self.query:
			print(data.id, data.name)
			for child in data.children:
				print(child.id, child.name)
			print()

	def consume_and_insert(self):
		self.connect_to_postgre()
		self.consume_data()
		self.create_session()
		# self.delete_removed_record()
		for message in self.consume_task:
			self.data = json.loads(message.value)
			print("Message: ",self.data)
			self.filter_object_by_name()
			self.upsert_object()
		
# example = {
#   "mimeType": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
#   "parents": [
#     "1a8eOWJNyR3uUrtFER2Vn0ZTh7yXUMqLc"
#   ],
#   "size": "9191",
#   "id": "1jCGgzuHYJjQDgt9u8Sr1Qblxn6eNVQwg",
#   "name": "Status Gizi Dewasa_2025.xlsx",
#   "modifiedTime": "2024-05-26T06:38:01.593Z",
#   "folderId": "1a8eOWJNyR3uUrtFER2Vn0ZTh7yXUMqLc",
#   "folder_name": "Apalah dia Apalah"
# }

# data = PropertiesConsumer()
# data.connect_to_postgre()
# data.data = example
# data.create_session()
# data.filter_object_by_name()
# # data.add_new_record()
# data.upsert_object()
# data.insert_to_postgre()