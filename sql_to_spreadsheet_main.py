import db_connection_ssh as db
import google_api as g_api

from google.oauth2.service_account import Credentials

from os import path, scandir
import json
import pandas as pd
import webbrowser

BASE_DIR = path.dirname(path.abspath(__file__))
SQL_DIR = path.join(BASE_DIR, 'sql_queries')

BASE_SPREADSHEET_URL = 'https://docs.google.com/spreadsheets/d/'
TEST_SPREADSHEET_ID = '1gj95USnjx5vcdECsJSt6L67LSFzm11Is1EgNOYtMqTY'
TEST_SHEET_ID = 590384370


def read_json(filepath):
    with open(file=filepath, mode='r') as file:
        return json.load(file)
    
    
creds_ssh = read_json(path.join(BASE_DIR, 'creds_ssh.json'))
creds_db = read_json(path.join(BASE_DIR, 'creds_db.json'))

ssh_host = creds_ssh['ssh_host']
ssh_user = creds_ssh['ssh_user']
ssh_port = creds_ssh['ssh_port']
pkey_file_path = creds_ssh['pkey_file_path']

sql_username = creds_db['sql_username']
sql_hostname = creds_db['sql_hostname']
sql_password = creds_db['sql_password']
sql_main_database = creds_db['sql_main_database']
sql_port = creds_db['sql_port']

server_tunnel = db.generate_ssh_tunnel(ssh_host, ssh_port, ssh_user, pkey_file_path, sql_hostname, sql_port)
db_connection = db.generate_db_connection(sql_username, sql_password, sql_main_database, server_tunnel)

credentials_google = Credentials.from_service_account_file(filename=path.join(BASE_DIR, 'creds_google.json'), scopes=g_api.SCOPES)
spreadsheet = g_api.create_spreadsheet_instance(credentials_google)
drive = g_api.create_drive_instance(credentials_google)

with scandir(SQL_DIR) as entries:
    for entry in entries:
        if entry.is_file() and entry.name.endswith('.sql'):
            
            query_name = entry.name[:-4] 
                       
            with open(file=entry.path, mode='r', encoding='utf-8') as file:
                query = file.read()

            df = pd.read_sql(query, db_connection)
            print(query_name, 'completed')
            
            df.columns = df.columns.str.replace('_', ' ', case=False)

            data_to_write = [df.columns.to_list()] + df.values.tolist()
            
            file_id_for_result = g_api.get_file_id_by_name(drive, query_name)
            if file_id_for_result:
                g_api.clear_values(spreadsheet, file_id_for_result, 'Sheet1')
                append_response = g_api.append_cell_values(spreadsheet, file_id_for_result, data_to_write, 'Sheet1', input_option='USER_ENTERED')
            else:
                file_id_for_result = g_api.create_new_spreadsheet(spreadsheet, query_name)
                g_api.share_file_for_anyone(drive, file_id_for_result, 'writer')
                append_response = g_api.append_cell_values(spreadsheet, file_id_for_result, data_to_write, 'Sheet1', input_option='USER_ENTERED')
        
            webbrowser.open(BASE_SPREADSHEET_URL + file_id_for_result)

g_api.show_all_files(drive)
server_tunnel.stop()
print('Done!')
