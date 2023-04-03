import db_connection_ssh as db
import google_api as g_api

from google.oauth2.service_account import Credentials

from os import path, scandir
import pandas as pd
import webbrowser

BASE_DIR = path.dirname(path.abspath(__file__))
SQL_DIR = path.join(BASE_DIR, 'sql_queries')

BASE_SPREADSHEET_URL = 'https://docs.google.com/spreadsheets/d/'

path_db_creds = path.join(BASE_DIR, 'creds_db_purchase.json')
path_ssh_creds = path.join(BASE_DIR, 'creds_ssh.json')
db_connection, ssh_tunnel = db.create_ssh_database_connection(path_db_creds, path_ssh_creds)

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
ssh_tunnel.stop()
print('Done!')
