import uuid
from datetime import datetime
import json
from sqlalchemy import create_engine, insert, text
import time
import os
from dotenv import load_dotenv
from datetime import timedelta
import time
from kafka import KafkaConsumer
import logging
from jogja_api.apis import *

load_dotenv()
default_args = {
	'owner': 'airscholar',
	'start_date': datetime.now()
}

def random_user_stream_data():
	import json
	from kafka import KafkaProducer
	import time
	import logging
	producer = KafkaProducer(bootstrap_servers=['broker-1:29093','broker:29092'])
	curr_time = time.time()

	while True:
		if time.time() > curr_time + 10: #1 minute
			break
		try:
			data = RandomNameAPI()
			data.get_data()
			data.format_data()
			logging.info("Sending Data: ",data.data)
			producer.send('users_created', json.dumps(data.data).encode('utf-8'))
		except Exception as e:
			logging.error(f'An error occured: {e}')
			continue
	
	producer.flush()
	producer.close()

def weather_stream_data():
	import json
	from kafka import KafkaProducer
	import time
	import logging
	producer = KafkaProducer(bootstrap_servers=['broker-1:29093','broker:29092'])
	data = WeatherAPI()
	data.get_data()
	data.format_data_daily()
	data.format_data_hourly()
	for row in data.daily_data:
		# logging.info("Sending Data: ",row)
		producer.send('daily_weather', json.dumps(row).encode('utf-8'))

	for row in data.hourly_data:
		# logging.info("Sending Data: ",row)
		producer.send('hourly_weather', json.dumps(row).encode('utf-8'))

	# except Exception as e:
	#     logging.error(f'An error occured: {e}')
	
	producer.flush()
	producer.close()


def forecast_weather_produce():
	import json
	from kafka import KafkaProducer
	import time
	import logging
	producer = KafkaProducer(bootstrap_servers=['broker-1:29093','broker:29092'])
	data = ForecastWeatherAPI()
	data.get_data()
	data.format_data_hourly()

	for row in data.hourly_data:
		# logging.info("Sending Data: ",row)
		producer.send('hourly_forecast_weather', json.dumps(row).encode('utf-8'))

	# except Exception as e:
	#     logging.error(f'An error occured: {e}')
	
	producer.flush()
	producer.close()

def delay_func():
	time.sleep(5)
