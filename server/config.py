import os
from pathlib import Path

from dotenv import load_dotenv


class Config:
    def __init__(self):
        # Look for .env file in the server directory
        server_dir = Path(__file__).parent
        env_path = server_dir / ".env"
        load_dotenv(env_path)
        
        self.tidb_host = os.getenv("TIDB_HOST", "127.0.0.1")
        self.tidb_port = int(os.getenv("TIDB_PORT", "4000"))
        self.tidb_user = os.getenv("TIDB_USER", "root")
        self.tidb_password = os.getenv("TIDB_PASS", "")
        self.tidb_db_name = os.getenv("TIDB_DB", "test")
        self.ca_path = os.getenv("CA_PATH", "")