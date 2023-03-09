from sqlalchemy import create_engine, engine
from sshtunnel import SSHTunnelForwarder
from dataclasses import dataclass

@dataclass
class SSH_credentials:
    ssh_host: str
    ssh_port: int
    ssh_user: str
    pkey_file_path: str


@dataclass
class SQL_credentials:
    sql_hostname: str
    sql_port: int
    sql_main_database: str
    sql_username: str
    sql_password: str


def generate_ssh_tunnel(credentials: SSH_credentials, remote_host, remote_port):
    tunnel = SSHTunnelForwarder(
        (credentials.ssh_host, credentials.ssh_port),
        ssh_username=credentials.ssh_user,
        ssh_pkey=credentials.pkey_file_path,
        remote_bind_address=(remote_host, remote_port)
    )
    tunnel.start()
    return tunnel


def generate_db_connection(credentials: SQL_credentials, ssh_tunnel):
    url = engine.url.URL(
        drivername='postgresql+psycopg2',
        username=credentials.sql_username,
        password=credentials.sql_password,
        host='127.0.0.1',
        port=ssh_tunnel.local_bind_port,
        database=credentials.sql_main_database
    )
    return create_engine(url)
