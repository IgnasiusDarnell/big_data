from kafka import KafkaConsumer
import json
from datetime import datetime
from sqlalchemy import create_engine, insert, text
import os
from dotenv import load_dotenv
import time
import logging
from datetime import date,timedelta
from batch_unstructured.py.models import *
from jogja_api.models import create_db as create_db2
# from confluent_kafka import Consumer

load_dotenv()

class RandomAPIConsumer():
	def __init__(self):
		self.default_args = {
			'owner': 'airscholar',
			'start_date': datetime.now()
		}
		self.topic_id = 'users_created'
		self.today_date = datetime.now() + timedelta(hours=7)

	def connect_to_postgre(self):
		self.engine = create_engine("postgresql+psycopg2://{}:{}@{}:{}/{}".format(
			os.getenv('LOCAL_POSTGRE_DB_USERNAME'),
			os.getenv('LOCAL_POSTGRE_DB_PASSWORD'),
			os.getenv('LOCAL_POSTGRE_DB_HOSTNAME'),
			os.getenv('LOCAL_POSTGRE_PORT'),
			os.getenv('LOCAL_POSTGRE_DB_DB_NAME'),
		))
		self.conn = self.engine.connect()
		create_db(engine=self.engine)
		create_db2(engine=self.engine)

	def consume_data(self):
		curr_time = time.time()
		
		self.consume_task = KafkaConsumer(self.topic_id,
									bootstrap_servers=['broker-1:29093','broker:29092'],
									auto_offset_reset="earliest",
									consumer_timeout_ms=30000,
									enable_auto_commit=True)
		self.consume_task.subscribe(topics=self.topic_id)

	def get_query(self):
		self.query=text("""INSERT INTO username_db
						(
						first_name,
						last_name,
						gender,
						address,
						post_code,
						email,
						username,
						dob,
						registered_date,
						phone,
						picture,
				  		last_refresh_date,
						country,
				  		state) VALUES
						(
						'{}',
						'{}',
						'{}',
						'{}',
						'{}',
						'{}',
						'{}',
						'{}',
						'{}',
						'{}',
						'{}',
				  		'{}',
						'{}',
				  		'{}')
						""".format(
									str(self.data['first_name']),
									str(self.data['last_name']),
									str(self.data['gender']),
									str(self.data['address']),
									str(self.data['post_code']),
									str(self.data['email']),
									str(self.data['username']),
									str(self.data['dob']),
									datetime.strptime(self.data['registered_date'],"%Y-%m-%dT%H:%M:%S.%fZ").date(),
									str(self.data['phone']),
									str(self.data['picture']),
									self.today_date,
									str(self.data['address']).split(', ')[-1],
									str(self.data['address']).split(', ')[-2]))
			
	def insert_to_postgre(self):
		for message in self.consume_task:
			self.data = json.loads(message.value)
			try:
				self.get_query()
				self.conn.execute(self.query)
			except:
				pass

	def consume_and_insert(self):
		self.connect_to_postgre()
		self.consume_data()
		self.insert_to_postgre()

class DailyWeatherAPIConsumer(RandomAPIConsumer):
	def __init__(self):
		super().__init__()
		self.topic_id = "daily_weather"

	def get_query(self):
		self.query=text("""INSERT INTO daily_weather_api_db
						(
						date,
						weather_code,
						temperature_2m_max,
						temperature_2m_min,
						temperature_2m_mean,
						daylight_duration,
						sunshine_duration,
						rain_sum,
						precipitation_hours,
				  		last_refresh_date) VALUES
						(
						'{}',
						'{}',
						'{}',
						'{}',
						'{}',
						'{}',
						'{}',
						'{}',
						'{}',
				  		'{}')
						""".format(
									datetime.strptime(self.data['date'],"%Y-%m-%d %H:%M:%S%z").date(),
									float(self.data['weather_code']),
									float(self.data['temperature_2m_max']),
									float(self.data['temperature_2m_min']),
									float(self.data['temperature_2m_mean']),
									float(self.data['daylight_duration']),
									float(self.data['sunshine_duration']),
									float(self.data['rain_sum']),
									float(self.data['precipitation_hours']),
									self.today_date))

class HourlyWeatherAPIConsumer(RandomAPIConsumer):
	def __init__(self):
		super().__init__()
		self.topic_id = "hourly_weather"

	def get_query(self):
		self.query=text("""INSERT INTO hourly_weather_api_db
						(
						date,
						temperature_2m,
						relative_humidity_2m,
						dew_point_2m,
						precipitation,
						rain,
						pressure_msl,
						surface_pressure,
						wind_speed_10m,
						sunshine_duration,
				  		last_refresh_date) VALUES
						(
						'{}',
						'{}',
						'{}',
						'{}',
						'{}',
						'{}',
						'{}',
						'{}',
						'{}',
						'{}',
						'{}')
						""".format(
									datetime.strptime(self.data['date'],"%Y-%m-%d %H:%M:%S%z"),
									float(self.data['temperature_2m']),
									float(self.data['relative_humidity_2m']),
									float(self.data['dew_point_2m']),
									float(self.data['precipitation']),
									float(self.data['rain']),
									float(self.data['pressure_msl']),
									float(self.data['surface_pressure']),
									float(self.data['wind_speed_10m']),
									float(self.data['sunshine_duration']),
									self.today_date))


class HourlyForecastWeatherAPIConsumer(RandomAPIConsumer):
	def __init__(self):
		super().__init__()
		self.topic_id = "hourly_forecast_weather"

	def get_query(self):
		self.query=text("""INSERT INTO forecast_hourly_weather_api_db
						(
						date,
						temperature_2m,
						relative_humidity_2m,
						dew_point_2m,
						precipitation,
						rain,
						pressure_msl,
						surface_pressure,
						wind_speed_10m,
						sunshine_duration,
				  		last_refresh_date) VALUES
						(
						'{}',
						'{}',
						'{}',
						'{}',
						'{}',
						'{}',
						'{}',
						'{}',
						'{}',
						'{}',
				  		'{}')
						""".format(
									datetime.strptime(self.data['date'],"%Y-%m-%d %H:%M:%S%z"),
									float(self.data['temperature_2m']),
									float(self.data['relative_humidity_2m']),
									float(self.data['dew_point_2m']),
									float(self.data['precipitation']),
									float(self.data['rain']),
									float(self.data['pressure_msl']),
									float(self.data['surface_pressure']),
									float(self.data['wind_speed_10m']),
									float(self.data['sunshine_duration']),
									self.today_date))

# with DAG('consume_user_automation',
#          default_args=default_args,
#          schedule_interval='@hourly',
#          catchup=False) as dag:

#     streaming_task = PythonOperator(
#         task_id='kafka_stream_1_consumer',
#         python_callable=consume
#     )