
from airflow import DAG
from airflow.operators.python import PythonOperator
from jogja_api.kafka_consume_stream import *
from jogja_api.kafka_produce_stream import *
from jogja_api.transform import *


# stream_data()
with DAG('produce_user_automation',
		 default_args=default_args,
		 schedule_interval='@daily',
		 catchup=False,
		 concurrency=2) as dag:

	produce_task_user = PythonOperator(
		task_id='kafka_stream_1_producer',
		python_callable=random_user_stream_data
	)
	consume_task_user = PythonOperator(
		task_id='kafka_stream_1_consumer',
		python_callable=RandomAPIConsumer().consume_and_insert
	)
	downstream_etl = PythonOperator(
		task_id='downstream_user_etl',
		python_callable=ETL().etl
	)

	produce_task_user >> consume_task_user >> downstream_etl

with DAG('weather_automation',
		 default_args=default_args,
		 schedule_interval='@daily',
		 catchup=False,
		 concurrency=2) as dag:
	
	produce_task_weather = PythonOperator(
		task_id='kafka_produce_weather',
		python_callable=weather_stream_data
	)    
	produce_forecast_task_weather = PythonOperator(
		task_id='kafka_forecast_produce_weather',
		python_callable=forecast_weather_produce
	)
	consume_task_daily_weather = PythonOperator(
		task_id='kafka_consume_daily_weather',
		python_callable=DailyWeatherAPIConsumer().consume_and_insert
	)
	consume_task_hourly_weather = PythonOperator(
		task_id='kafka_consume_hourly_weather',
		python_callable=HourlyWeatherAPIConsumer().consume_and_insert
	)
	consume_forecast_task_hourly_weather = PythonOperator(
		task_id='kafka_forecast_consume_hourly_weather',
		python_callable=HourlyForecastWeatherAPIConsumer().consume_and_insert
	)

	produce_task_weather >> produce_forecast_task_weather >> [consume_task_daily_weather,consume_task_hourly_weather,consume_forecast_task_hourly_weather]
