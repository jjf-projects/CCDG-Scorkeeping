# python & 3rd party
import os.path, datetime
from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker
# custom
from sql_db.models import Base
import logger.logger as logger

# Make this a Class


db_path = None

def init_db(db_settings) -> Session:
    ''' returns a sqlite connection as sqlqlchemy sessionameker='''
    global db_path 
    db_path= os.path.join(db_settings['DB_DIR'], db_settings['DB_NAME'])
    engine = create_engine(f"sqlite:///{db_path}", echo=db_settings['ECHO'])
    
    # Create tables
    Base.metadata.create_all(engine)
    
    # Create a session factory
    SessionLocal = sessionmaker(bind=engine)

    # if all that worked, clear the logs - we are starting fresh
    logger.clear_logs()
    logger.delete_log_files()

    return SessionLocal()

def get_db_last_update() -> datetime:
    ''' returns datetime for the last filesys update to the sqlite.bd file'''
    return os.path.getmtime(db_path)
    
