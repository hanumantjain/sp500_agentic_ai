from sqlalchemy import create_engine
from sqlalchemy.engine import URL
from sqlalchemy.orm import sessionmaker
import os
import sys

# Add the database directory to the path to find config
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from config.config import Config


def get_db_engine():
    config = Config()
    dsn = URL.create(
        drivername="mysql+pymysql",
        username=config.tidb_user,
        password=config.tidb_password,
        host=config.tidb_host,
        port=config.tidb_port,
        database=config.tidb_db_name,
    )
    connect_args = {}
    if config.ca_path:
        connect_args = {
            "ssl_verify_cert": True,
            "ssl_verify_identity": True,
            "ssl_ca": config.ca_path,
        }
    return create_engine(dsn, connect_args=connect_args)


# Reusable engine and session factory
engine = get_db_engine()
Session = sessionmaker(bind=engine)
