from batch_unstructured.py.queries import *
from jogja_api.models import *
from sqlalchemy import func,inspect

class ETL(PropertiesConsumer):
	def filter_object_by_name(self):
		# self.is_object_exists = self.started_session.query(google_drive_properties_db).filter(exists(select(1).from_(goo))).exists()
		self.selected_object = self.started_session.query(monthly_user_summary_db).filter(monthly_user_summary_db.registered_date == self.current_row[0],
																					monthly_user_summary_db.gender == self.current_row[1],
																					monthly_user_summary_db.country == self.current_row[2],
																					monthly_user_summary_db.state == self.current_row[3])

	def add_new_record(self):
		new_data = monthly_user_summary_db(registered_date = self.current_row[0],
										gender = self.current_row[1],
										country = self.current_row[2],
										state = self.current_row[3],
										count = self.current_row[4],
										start_of_registered_date = self.start_of_registered_date,
										cummulative_user_total = self.cumm_user_count,
										last_refresh_date = self.today_date)
		# registered_date = Column(Date)
		# gender = Column(String)
		# country = Column(String)
		# state = Column(String)
		# count = Column(Integer)
		# start_of_registered_date = Column(Date)
		# cummulative_user_total = Column(Integer)
		# last_refresh_date = Column(DateTime)
		
		self.started_session.add(new_data)
		self.started_session.commit()

	def update_object(self):
		self.filter_object_by_name()
		if(self.selected_object.count()>0):
			temp = self.selected_object.first()
			if(not(temp.registered_date == self.current_row[0] and temp.count == self.current_row[4] and temp.cummulative_user_total == self.cumm_user_count )):
				print("update data")
				temp.registered_date = self.current_row[0]
				temp.gender = self.current_row[1]
				temp.country = self.current_row[2]
				temp.state = self.current_row[3]
				temp.count = self.current_row[4]
				temp.start_of_registered_date = self.start_of_registered_date
				temp.cummulative_user_total = self.cumm_user_count
				temp.last_refresh_date = self.today_date
				self.started_session.commit()

	def upsert_object(self):
		self.update_object()
		if(self.selected_object.count()==0):
			print("ada data baru")
			self.add_new_record()

	def etl(self):
		self.connect_to_postgre()
		self.create_session()
		create_db(self.engine)
		self.data = self.started_session.query(username_db.registered_date,
											username_db.gender,
											username_db.country,
											username_db.state,func.count(username_db.gender)).group_by(username_db.registered_date,
																					username_db.gender,
																					username_db.country,
																					username_db.state).order_by(username_db.registered_date).all()
		# self.data = self.data.order_by
		self.cumm_user_count = 0
		for item in self.data:
			self.current_row = item
			self.cumm_user_count += self.current_row[4]
			print(self.current_row)
			# print(str(self.current_row[0].year))
			# print(str(self.current_row[0].month))
			# print(datetime.strptime(str(self.current_row[0].year) + "-" + str(self.current_row[0].month) + "-1","%Y-%m-%d").date())
			self.start_of_registered_date = datetime.strptime(str(self.current_row[0].year) + "-" + str(self.current_row[0].month) + "-1","%Y-%m-%d").date()
			self.upsert_object()
		
# session.query(Table.column, 
#    func.count(Table.column)).group_by(Table.column).all()
# 		for item in self.data:
# 			self.current_row = item

		
