# python & 3rd party
import os.path, datetime
from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker
# custom
from sql_db.models import Base
from logger.logger import logger_gen as logger
from logger.logger import delete_log_files

# Make this a Class


db_path = None

def init_db(db_path: str, echo: bool = False) -> Session:
    ''' returns a sqlite connection as sqlqlchemy sessionameker='''

    print(f"Database: {db_path}")
    logger.info(f"Database: {db_path}")
    engine = create_engine(f"sqlite:///{db_path}", echo=echo)
    
    # Create tables
    Base.metadata.create_all(engine)
    
    # Create a session factory
    SessionLocal = sessionmaker(bind=engine)

    # if all that worked, clear the logs - we are starting fresh
    delete_log_files()

    return SessionLocal()

def get_db_last_update(db: Session) -> datetime:
    ''' returns datetime for the last filesys update to the sqlite.bd file'''
    db_path = get_absolute_db_path(db)
    return os.path.getmtime(db_path)

def get_absolute_db_path(session: Session) -> str:
    engine = session.get_bind()
    url = engine.url

    if url.drivername == "sqlite":
        if url.database == ":memory:":
            return ":memory:"
        return os.path.abspath(url.database)

    raise ValueError("This function only supports SQLite databases.")
    
