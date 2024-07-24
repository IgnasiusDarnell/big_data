from sqlalchemy.orm import sessionmaker,scoped_session
from datetime import datetime
import sys
# sys.path.append("E:\\Exploration\\python\\freelance\\single-kafka\\big-data-freelance\\dags\\jogja_api")
from jogja_api.kafka_consume_stream import *
from sqlalchemy import create_engine, insert, text,exists,select
from batch_unstructured.py.models import *
from batch_unstructured.py.queries import *
from batch_unstructured.py.minio_api import batch_DAG
import os
from io import StringIO

class generalProcessCSVFile(batch_DAG,PropertiesConsumer):
	def __init__(self):
		super().__init__()

	def connect_to_postgre(self):
		self.engine = create_engine("postgresql+psycopg2://{}:{}@{}:{}/{}".format(
			os.getenv('LOCAL_POSTGRE_DB_USERNAME'),
			os.getenv('LOCAL_POSTGRE_DB_PASSWORD'),
			os.getenv('LOCAL_POSTGRE_DB_HOSTNAME'),
			os.getenv('LOCAL_POSTGRE_PORT'),
			os.getenv('LOCAL_POSTGRE_DB_DB_NAME'),
		))
		self.conn = self.engine.connect()
		  
	def create_session(self):
		self.session = sessionmaker(bind=self.engine)
		self.scoped_sess = scoped_session(self.session)
		self.started_session = self.session()
		  
	def find_csv_file_in_minio(self):

		pass

	def download_file_from_minio(self):
		self.downloaded_obj = self.minio_client.get_object(
			bucket_name=self.bucket_name,
			object_name="{}/{}".format(self.file_data.folder_name,self.file_data.file_name),
		)

	def read_csv(self):
		import csv
		file = StringIO(self.downloaded_obj.data.decode())
		csv_data = csv.reader(file,delimiter=',')
		row_id = 0
		for row in csv_data:
			self.selected_row = row
			if(row_id == 0):
				self.selected_row.append("file_name")
			else:
				self.selected_row.append(self.file_data.file_name)
				self.upsert_object()
			row_id+=1

	def filter_object_by_name(self):
		# self.is_object_exists = self.started_session.query(google_drive_properties_db).filter(exists(select(1).from_(goo))).exists()
		self.selected_object = self.started_session.query(kesehatan_tenaga_kerja_db).filter(kesehatan_tenaga_kerja_db.jenis_nakes == self.selected_row[0],
																					  kesehatan_tenaga_kerja_db.kode_provinsi == self.selected_row[1],
																					  kesehatan_tenaga_kerja_db.nama_provinsi == self.selected_row[2],
																					  kesehatan_tenaga_kerja_db.puskesmas == self.selected_row[3],
																					  kesehatan_tenaga_kerja_db.rumah_sakit == self.selected_row[4],
																					  kesehatan_tenaga_kerja_db.faskes_lainnya == self.selected_row[5],
																					  kesehatan_tenaga_kerja_db.diperbarui_pada == self.selected_row[6],
																					  kesehatan_tenaga_kerja_db.file_name == self.selected_row[7])

	def add_new_record(self):
		new_data = kesehatan_tenaga_kerja_db(jenis_nakes = self.selected_row[0],
										kode_provinsi = self.selected_row[1],
										nama_provinsi = self.selected_row[2],
										puskesmas = self.selected_row[3],
										rumah_sakit = self.selected_row[4],
										faskes_lainnya = self.selected_row[5],
										diperbarui_pada = self.selected_row[6],
										file_name = self.selected_row[7])
		
		self.started_session.add(new_data)
		self.started_session.commit()
		
	def update_object(self):
		self.filter_object_by_name()
		if(self.selected_object.count()>0):
			temp = self.selected_object.first()
			if(temp.diperbarui_pada != self.selected_row[6]):
				temp.jenis_nakes = self.selected_row[0]
				temp.kode_provinsi = self.selected_row[1]
				temp.nama_provinsi = self.selected_row[2]
				temp.puskesmas = self.selected_row[3]
				temp.rumah_sakit = self.selected_row[4]
				temp.faskes_lainnya = self.selected_row[5]
				temp.diperbarui_pada = self.selected_row[6]
				temp.file_name = self.selected_row[7]
				self.started_session.commit()

	def upsert_object(self):
		self.update_object()
		if(self.selected_object.count()==0):
			self.add_new_record()

	def download_and_read_csv(self):
		for file_data in self.filtered_object:
			self.file_data = file_data
			self.download_file_from_minio()
			self.read_csv()

	def filter_object_by_file_type(self):
		self.filtered_object = self.started_session.query(google_drive_properties_db).filter(google_drive_properties_db.mimetype.contains("csv"),
																							google_drive_properties_db.folder_name.contains("Tenaga Kesehatan Berdasarkan Kategori"))
	
	def operate(self):
		self.connect_to_postgre()
		self.connect_to_minio()
		self.create_session()
		self.filter_object_by_file_type()
		self.download_and_read_csv()

class RegisteredNakesProcessCSVFile(generalProcessCSVFile):
	def __init__(self):
		super().__init__()

	def filter_object_by_name(self):
		# self.is_object_exists = self.started_session.query(google_drive_properties_db).filter(exists(select(1).from_(goo))).exists()
		self.selected_object = self.started_session.query(registered_tenaga_kesehatan).filter(registered_tenaga_kesehatan.profesi == self.selected_row[0],
																					  registered_tenaga_kesehatan.jumlah_nakes_teregistrasi == self.selected_row[1],
																					  registered_tenaga_kesehatan.diperbarui_pada == self.selected_row[2])
		
	def add_new_record(self):
		new_data = registered_tenaga_kesehatan(profesi = self.selected_row[0],
										jumlah_nakes_teregistrasi = self.selected_row[1],
										diperbarui_pada = self.selected_row[2])
		
		self.started_session.add(new_data)
		self.started_session.commit()
		
	def update_object(self):
		self.filter_object_by_name()
		if(self.selected_object.count()>0):
			temp = self.selected_object.first()
			if(temp.diperbarui_pada != self.selected_row[2]):
				temp.profesi = self.selected_row[0]
				temp.jumlah_nakes_teregistrasi = self.selected_row[1]
				temp.diperbarui_pada = self.selected_row[2]
				self.started_session.commit()

	def filter_object_by_file_type(self):
		self.filtered_object = self.started_session.query(google_drive_properties_db).filter(google_drive_properties_db.mimetype.contains("csv"),
																							google_drive_properties_db.folder_name.contains("Summary Tenaga Kesehatan"))