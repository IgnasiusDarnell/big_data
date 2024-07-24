import uuid
import openmeteo_requests
import requests_cache
import pandas as pd
from retry_requests import retry
from datetime import datetime
from datetime import timedelta

class GeneralAPI():
	def __init__(self):
		self.url = None

	def get_data(self):
		import requests

		self.res = requests.get(self.url)
		self.res = self.res.json()

	def format_data(self):
		self.data = {}
		location = self.res['location']
		self.data['id'] = str(uuid.uuid4())
		self.data['first_name'] = self.res['name']['first']
		self.data['last_name'] = self.res['name']['last']
		self.data['gender'] = self.res['gender']
		self.data['address'] = f"{str(location['street']['number'])} {location['street']['name']}, " \
						f"{location['city']}, {location['state']}, {location['country']}"
		self.data['post_code'] = str(location['postcode'])
		self.data['email'] = self.res['email']
		self.data['username'] = self.res['login']['username']
		self.data['dob'] = self.res['dob']['date']
		self.data['registered_date'] = self.res['registered']['date']
		self.data['phone'] = self.res['phone']
		self.data['picture'] = self.res['picture']['medium']

class RandomNameAPI(GeneralAPI):
	def __init__(self):
		self.url = "https://randomuser.me/api/"

	def get_data(self):
		super().get_data()
		self.res = self.res['results'][0]

class JogjaAPI(GeneralAPI):
	def __init__(self):
		self.url = "https://opendata.jogjakota.go.id/data/pajak/pad_bulan_seriesapi/index/2024--JAN--MAR--7"

		print(self.get_data())

	def format_data(self):
		self.data = {}


class WeatherAPI(GeneralAPI):
	def __init__(self):
		self.url = "https://archive-api.open-meteo.com/v1/archive"
		self.today_date = datetime.now()
		self.params = {
			"latitude": -7.7156,
			"longitude": 110.3556,
			"start_date": (datetime.now() - timedelta(days=2)).strftime("%Y-%m-%d"),
			"end_date": (datetime.now()).strftime("%Y-%m-%d"),
			"hourly": ["temperature_2m", "relative_humidity_2m", "dew_point_2m", "apparent_temperature", "precipitation", "rain", "pressure_msl", "surface_pressure", "wind_speed_10m", "sunshine_duration"],
			"daily": ["weather_code", "temperature_2m_max", "temperature_2m_min", "temperature_2m_mean", "daylight_duration", "sunshine_duration", "rain_sum", "precipitation_hours"],
			"timezone": "Asia/Singapore"
		}

		print(self.get_data())

	def get_data(self):
		cache_session = requests_cache.CachedSession('.cache', expire_after = -1)
		retry_session = retry(cache_session, retries = 5, backoff_factor = 0.2)
		self.openmeteo = openmeteo_requests.Client(session = retry_session)
		
		self.data = self.openmeteo.weather_api(self.url, params=self.params)
		self.data = self.data[0]

	def format_data_daily(self):
		daily = self.data.Daily()
		daily_weather_code = daily.Variables(0).ValuesAsNumpy()
		daily_temperature_2m_max = daily.Variables(1).ValuesAsNumpy()
		daily_temperature_2m_min = daily.Variables(2).ValuesAsNumpy()
		daily_temperature_2m_mean = daily.Variables(3).ValuesAsNumpy()
		daily_daylight_duration = daily.Variables(4).ValuesAsNumpy()
		daily_sunshine_duration = daily.Variables(5).ValuesAsNumpy()
		daily_rain_sum = daily.Variables(6).ValuesAsNumpy()
		daily_precipitation_hours = daily.Variables(7).ValuesAsNumpy()

		self.daily_data = {"date": pd.Series(pd.date_range(
			start = pd.to_datetime(daily.Time(), unit = "s", utc = True),
			end = pd.to_datetime(daily.TimeEnd(), unit = "s", utc = True),
			freq = pd.Timedelta(seconds = daily.Interval()),
			inclusive = "left"
		).format())}
		self.daily_data["weather_code"] = daily_weather_code
		self.daily_data["temperature_2m_max"] = daily_temperature_2m_max
		self.daily_data["temperature_2m_min"] = daily_temperature_2m_min
		self.daily_data["temperature_2m_mean"] = daily_temperature_2m_mean
		self.daily_data["daylight_duration"] = daily_daylight_duration
		self.daily_data["sunshine_duration"] = daily_sunshine_duration
		self.daily_data["rain_sum"] = daily_rain_sum
		self.daily_data["precipitation_hours"] = daily_precipitation_hours

		self.daily_data = [{
			"date":self.daily_data["date"][i],
			"weather_code":str(self.daily_data['weather_code'][i]),
			"temperature_2m_max":str(self.daily_data['temperature_2m_max'][i]),
			"temperature_2m_min":str(self.daily_data['temperature_2m_min'][i]),
			"temperature_2m_mean":str(self.daily_data['temperature_2m_mean'][i]),
			"daylight_duration":str(self.daily_data['daylight_duration'][i]),
			"sunshine_duration":str(self.daily_data['sunshine_duration'][i]),
			"rain_sum":str(self.daily_data['rain_sum'][i]),
			"precipitation_hours":str(self.daily_data['precipitation_hours'][i]),
		} for i in range(len(self.daily_data[list(self.daily_data.keys())[0]]))]
		
	def format_data_hourly(self):
		# Process hourly data. The order of variables needs to be the same as requested.
		hourly = self.data.Hourly()
		hourly_temperature_2m = hourly.Variables(0).ValuesAsNumpy()
		hourly_relative_humidity_2m = hourly.Variables(1).ValuesAsNumpy()
		hourly_dew_point_2m = hourly.Variables(2).ValuesAsNumpy()
		hourly_apparent_temperature = hourly.Variables(3).ValuesAsNumpy()
		hourly_precipitation = hourly.Variables(4).ValuesAsNumpy()
		hourly_rain = hourly.Variables(5).ValuesAsNumpy()
		hourly_pressure_msl = hourly.Variables(6).ValuesAsNumpy()
		hourly_surface_pressure = hourly.Variables(7).ValuesAsNumpy()
		hourly_wind_speed_10m = hourly.Variables(8).ValuesAsNumpy()
		hourly_sunshine_duration = hourly.Variables(9).ValuesAsNumpy()

		self.hourly_data = {"date": pd.Series(pd.date_range(
			start = pd.to_datetime(hourly.Time(), unit = "s", utc = True),
			end = pd.to_datetime(hourly.TimeEnd(), unit = "s", utc = True),
			freq = pd.Timedelta(seconds = hourly.Interval()),
			inclusive = "left"
		).format())}
		self.hourly_data["temperature_2m"] = hourly_temperature_2m
		self.hourly_data["relative_humidity_2m"] = hourly_relative_humidity_2m
		self.hourly_data["dew_point_2m"] = hourly_dew_point_2m
		self.hourly_data["apparent_temperature"] = hourly_apparent_temperature
		self.hourly_data["precipitation"] = hourly_precipitation
		self.hourly_data["rain"] = hourly_rain
		self.hourly_data["pressure_msl"] = hourly_pressure_msl
		self.hourly_data["surface_pressure"] = hourly_surface_pressure
		self.hourly_data["wind_speed_10m"] = hourly_wind_speed_10m
		self.hourly_data["sunshine_duration"] = hourly_sunshine_duration

		self.hourly_data = [{
			"date":self.hourly_data["date"][i],
			"temperature_2m":str(self.hourly_data['temperature_2m'][i]),
			"relative_humidity_2m":str(self.hourly_data['relative_humidity_2m'][i]),
			"dew_point_2m":str(self.hourly_data['dew_point_2m'][i]),
			"apparent_temperature":str(self.hourly_data['apparent_temperature'][i]),
			"precipitation":str(self.hourly_data['precipitation'][i]),
			"rain":str(self.hourly_data['rain'][i]),
			"pressure_msl":str(self.hourly_data['pressure_msl'][i]),
			"surface_pressure":str(self.hourly_data['surface_pressure'][i]),
			"wind_speed_10m":str(self.hourly_data['wind_speed_10m'][i]),
			"sunshine_duration":str(self.hourly_data['sunshine_duration'][i]),
		} for i in range(len(self.hourly_data[list(self.hourly_data.keys())[0]]))]


class ForecastWeatherAPI(WeatherAPI):
	def __init__(self):
		self.url = "https://api.open-meteo.com/v1/forecast"
		self.today_date = datetime.now()
		self.params = {
			"latitude": -7.7156,
			"longitude": 110.3556,
			"hourly": ["temperature_2m", "relative_humidity_2m", "dew_point_2m", "apparent_temperature", "precipitation", "rain", "pressure_msl", "surface_pressure", "wind_speed_10m", "sunshine_duration"],
			"timezone": "Asia/Singapore",
			"forecast_days": 7
		}