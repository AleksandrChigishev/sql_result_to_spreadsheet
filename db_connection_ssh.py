from sqlalchemy import create_engine, engine
from sshtunnel import SSHTunnelForwarder

def generate_ssh_tunnel(host, port, user, private_key, remote_host, remote_port):
    tunnel = SSHTunnelForwarder(
        (host, port),
        ssh_username=user,
        ssh_pkey=private_key,
        remote_bind_address=(remote_host, remote_port)
    )
    tunnel.start()
    return tunnel


def generate_db_connection(username, password, db_name, ssh_tunnel):
    url = engine.url.URL(
        drivername='postgresql+psycopg2',
        username=username,
        password=password,
        host='127.0.0.1',
        port=ssh_tunnel.local_bind_port,
        database=db_name
    )
    return create_engine(url)
