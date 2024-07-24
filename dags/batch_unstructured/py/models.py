from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import create_engine, Column, Integer, String,Date,DateTime,inspect

Base = declarative_base()

class google_drive_properties_db(Base):
    __tablename__ = 'storage_properties_db'
    id = Column(Integer, primary_key=True)
    folderid = Column(String)
    folder_name = Column(String)
    fileid = Column(String)
    file_name = Column(String)
    mimetype = Column(String)
    modified_time = Column(String)
    size = Column(String)
    last_refresh_date = Column(DateTime)


class kesehatan_tenaga_kerja_db(Base):
    __tablename__ = 'tenaga_kesehatan_biomedika_db'
    id = Column(Integer, primary_key=True)
    jenis_nakes = Column(String)
    kode_provinsi = Column(String)
    nama_provinsi = Column(String)
    puskesmas = Column(Integer)
    rumah_sakit = Column(Integer)
    faskes_lainnya = Column(Integer)
    diperbarui_pada = Column(Date)
    file_name = Column(String)


class registered_tenaga_kesehatan(Base):
    __tablename__ = 'registered_tenaga_kesehatan_db'
    id = Column(Integer, primary_key=True)
    profesi = Column(String)
    jumlah_nakes_teregistrasi = Column(Integer)
    diperbarui_pada = Column(Date)


def create_db(engine):
    if(not (inspect(engine).has_table(table_name="registered_tenaga_kesehatan_db",schema="public"))
       or not (inspect(engine).has_table(table_name="tenaga_kesehatan_biomedika_db",schema="public"))
        or not (inspect(engine).has_table(table_name="storage_properties_db",schema="public"))):
        Base.metadata.create_all(engine)
    else:
        print("All database is created")