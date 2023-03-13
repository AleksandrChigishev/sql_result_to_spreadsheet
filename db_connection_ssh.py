from sqlalchemy import create_engine, engine
from sshtunnel import SSHTunnelForwarder
from dataclasses import dataclass
import json

@dataclass
class SSH_credentials:
    host: str
    port: int
    user: str
    private_key_file_path: str

@dataclass
class SQL_credentials:
    hostname: str
    port: int
    database: str
    username: str
    password: str


def read_json(filepath):
    with open(file=filepath, mode='r') as file:
        return json.load(file)


def generate_ssh_tunnel(credentials: SSH_credentials, remote_host, remote_port):
    tunnel = SSHTunnelForwarder(
        (credentials.host, credentials.port),
        ssh_username=credentials.user,
        ssh_pkey=credentials.private_key_file_path,
        remote_bind_address=(remote_host, remote_port)
    )
    tunnel.start()
    return tunnel


def generate_db_connection(credentials: SQL_credentials, ssh_tunnel):
    url = engine.url.URL(
        drivername='postgresql+psycopg2',
        username=credentials.username,
        password=credentials.password,
        host='127.0.0.1',
        port=ssh_tunnel.local_bind_port,
        database=credentials.database
    )
    return create_engine(url)


def create_ssh_database_connection(database_credentials_path, ssh_credentials_path):
    
    creds_db = SQL_credentials(**read_json(database_credentials_path))
    creds_ssh = SSH_credentials(**read_json(ssh_credentials_path))
    
    server_tunnel = generate_ssh_tunnel(creds_ssh, remote_host=creds_db.hostname, remote_port=creds_db.port)
    return generate_db_connection(creds_db, ssh_tunnel=server_tunnel), server_tunnel