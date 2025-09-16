from sqlalchemy import create_engine, text
from sqlalchemy.engine import URL
from sqlalchemy.orm import sessionmaker
from config import Config
import certifi


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
    # Enable TLS for TiDB (PyMySQL expects an 'ssl' dict)
    ca_file = config.ca_path if config.ca_path else certifi.where()
    connect_args = {"ssl": {"ca": ca_file}}
    return create_engine(dsn, connect_args=connect_args)


# Reusable engine and session factory
engine = get_db_engine()
Session = sessionmaker(bind=engine)


def run_query(query: str, params=None):
    with engine.connect() as conn:
        if params:
            if isinstance(params, (list, tuple)):
                result = conn.execute(text(query), params)
            else:
                result = conn.execute(text(query), [params])
        else:
            result = conn.execute(text(query))
        # SQLAlchemy Row -> dict for JSON serialization
        return [dict(row._mapping) for row in result]