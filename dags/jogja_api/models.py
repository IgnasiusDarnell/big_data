from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import create_engine, Column, Integer, String,Date,DateTime,inspect,Float

Base = declarative_base()

class kesehatan_tenaga_kerja_summary_db(Base):
    __tablename__ = 'tenaga_kesehatan_biomedika_db'
    id = Column(Integer, primary_key=True)
    jenis_nakes = Column(String)
    kode_provinsi = Column(String)
    nama_provinsi = Column(String)
    puskesmas = Column(String)
    rumah_sakit = Column(String)
    faskes_lainnya = Column(String)
    diperbarui_pada = Column(Date)
    file_name = Column(String)
    last_refresh_date = Column(Date)

class username_db(Base):
    __tablename__ = 'username_db'
    id = Column(Integer, primary_key=True)
    first_name = Column(String)
    last_name = Column(String)
    gender = Column(String)
    address = Column(String)
    post_code = Column(String)
    email = Column(String)
    username = Column(String)
    dob = Column(String)
    registered_date = Column(Date)
    phone = Column(String)
    picture = Column(String)
    last_refresh_date = Column(DateTime)
    country = Column(String)
    state = Column(String)

class monthly_user_summary_db(Base):
    __tablename__ = 'monthly_user_summary_db'
    id = Column(Integer, primary_key=True)
    registered_date = Column(Date)
    gender = Column(String)
    country = Column(String)
    state = Column(String)
    count = Column(Integer)
    start_of_registered_date = Column(Date)
    cummulative_user_total = Column(Integer)
    last_refresh_date = Column(DateTime)

class daily_weather_api_db(Base):
    __tablename__ = 'daily_weather_api_db'
    id = Column(Integer, primary_key=True)
    date = Column(Date)
    weather_code = Column(Float)
    temperature_2m_max = Column(Float)
    temperature_2m_min = Column(Float)
    temperature_2m_mean = Column(Float)
    daylight_duration = Column(Float)
    sunshine_duration = Column(Float)
    rain_sum = Column(Float)
    precipitation_hours = Column(Float)
    last_refresh_date = Column(DateTime)
    
class hourly_weather_api_db(Base):
    __tablename__ = 'hourly_weather_api_db'
    id = Column(Integer, primary_key=True)
    date = Column(DateTime)
    temperature_2m = Column(Float)
    relative_humidity_2m = Column(Float)
    dew_point_2m = Column(Float)
    precipitation = Column(Float)
    rain = Column(Float)
    pressure_msl = Column(Float)
    surface_pressure = Column(Float)
    wind_speed_10m = Column(Float)
    sunshine_duration = Column(Float)
    last_refresh_date = Column(DateTime)
    
class forecast_hourly_weather_api_db(Base):
    __tablename__ = 'forecast_hourly_weather_api_db'
    id = Column(Integer, primary_key=True)
    date = Column(DateTime)
    temperature_2m = Column(Float)
    relative_humidity_2m = Column(Float)
    dew_point_2m = Column(Float)
    precipitation = Column(Float)
    rain = Column(Float)
    pressure_msl = Column(Float)
    surface_pressure = Column(Float)
    wind_speed_10m = Column(Float)
    sunshine_duration = Column(Float)
    last_refresh_date = Column(DateTime)

def create_db(engine):
    if(not (inspect(engine).has_table(table_name="username_db",schema="public"))
       or not (inspect(engine).has_table(table_name="monthly_user_summary_db",schema="public"))
       or not (inspect(engine).has_table(table_name="tenaga_kesehatan_biomedika_db",schema="public"))
       or not (inspect(engine).has_table(table_name="hourly_weather_api_db",schema="public"))
       or not (inspect(engine).has_table(table_name="forecast_hourly_weather_api_db",schema="public"))
       or not (inspect(engine).has_table(table_name="daily_weather_api_db",schema="public"))):
        Base.metadata.create_all(engine)
    else:
        print("All database is created")