import os
from contextlib import contextmanager
from sqlalchemy import create_engine, text
from sqlalchemy.orm import Session

from sql_db.models import Base
from logger.logger import logger_gen as logger


@contextmanager
def get_session(db_path: str, echo: bool = False):
    """Context manager that yields a SQLAlchemy Session and ensures it is closed on exit.

    Usage:
        with ccdg_db.get_session(db_file_path, echo=False) as db:
            # use db here

    Args:
        db_path: Absolute path to the SQLite .db file. Created if it does not exist.
        echo:    If True, SQLAlchemy will log all SQL statements (useful for debugging).
    """
    logger.info(f"Database: {db_path}")
    engine = create_engine(f"sqlite:///{db_path}", echo=echo)
    Base.metadata.create_all(engine)    # create tables if they don't exist yet
    session = Session(engine)
    try:
        yield session
    finally:
        session.close()


def migrate(db_path: str) -> None:
    """Apply any schema changes that can't be handled by create_all().

    SQLAlchemy's create_all() creates missing tables but does NOT add new
    columns to existing tables.  Run this once after updating models.py
    mid-season (i.e. when the DB already has data you want to keep).

    Each migration step is idempotent — safe to run multiple times.

    Args:
        db_path: Absolute path to the SQLite .db file.
    """
    engine = create_engine(f"sqlite:///{db_path}")

    # Map of: (table, column) → ALTER TABLE statement to add it
    migrations = {
        ('score', 'hole_scores'): "ALTER TABLE score ADD COLUMN hole_scores TEXT",
    }

    with engine.connect() as conn:
        for (table, column), sql in migrations.items():
            # Check whether the column already exists before trying to add it
            existing = [
                row[1] for row in conn.execute(text(f"PRAGMA table_info({table})"))
            ]
            if column not in existing:
                conn.execute(text(sql))
                conn.commit()
                logger.info(f"Migration applied: added '{column}' to '{table}'.")
            else:
                logger.info(f"Migration skipped: '{column}' already exists in '{table}'.")


def get_db_path(session: Session) -> str:
    """Returns the absolute filesystem path of the SQLite database backing this session."""
    engine = session.get_bind()
    url = engine.url
    if url.drivername != "sqlite":
        raise ValueError("This function only supports SQLite databases.")
    if url.database == ":memory:":
        return ":memory:"
    return os.path.abspath(url.database)
