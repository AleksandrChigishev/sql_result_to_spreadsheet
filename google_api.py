from googleapiclient.discovery import build
import webbrowser
import re

SCOPES = ['https://www.googleapis.com/auth/drive', 'https://www.googleapis.com/auth/spreadsheets']

"""Google Spreadsheet API functions"""

def create_spreadsheet_instance(credentials):  
  service = build('sheets', 'v4', credentials=credentials)
  return service.spreadsheets()


def create_new_spreadsheet(spreadsheet_instance, title):
  spreadsheet_properties = {
    'properties' : {
      'title': title
    }
  }
  spreadsheet = spreadsheet_instance.create(body=spreadsheet_properties).execute()
  return spreadsheet.get('spreadsheetId')


def get_spreadsheet_information(spreadsheet_instance, spreadsheet_id, fields=None):
    return spreadsheet_instance.get(spreadsheetId=spreadsheet_id, fields=fields).execute()
  

def get_sheets_list(spreadsheet_instance, spreadsheet_id):
  fields = 'sheets.properties.title,sheets.properties.sheetId'
  response = spreadsheet_instance.get(spreadsheetId=spreadsheet_id, fields=fields).execute()
  sheets_properties_list = response.get('sheets')
  return list(map(lambda element: element.get('properties'), sheets_properties_list))


def get_sheet_id(spreadsheet_instance, spreadsheet_id, sheet_name):
  sheets = get_sheets_list(spreadsheet_instance, spreadsheet_id)  
  for sheet in sheets:
    if sheet.get('title') == sheet_name:
      return sheet.get('sheetId')    
  return None
  

def batch_update_spreadsheet(spreadsheet_instance, spreadsheet_id, requests):
    body = {'requests': requests}
    return spreadsheet_instance.batchUpdate(spreadsheetId=spreadsheet_id, body=body).execute()
  
    
def column_number_to_letter(column):
    column += 1
    letter = ''
    
    while column > 0:
        mod = (column - 1) % 26
        letter = chr(mod + 65) + letter
        column = (column - mod - 1) // 26
    return letter

  
def column_letter_to_number(letters):
  number = 0
  for i, letter in enumerate(letters[::-1]):
    number += pow(26, i) * (ord(letter) + 1 - 65) 
  return number - 1


def range_to_indexes(range):  
  sheet_name = range.split('!')[0]
  dimensions = range.split('!')[1]

  rows = re.findall(r'\d+', dimensions)
  if rows and len(rows) == 2:
    start_row, finish_row = map(int, rows)

  columns = re.findall(r'[A-Z]+', dimensions)
  if columns and len(columns) == 2:
    start_column, finish_column = columns
    
  start_column_index = column_letter_to_number(start_column)
  finish_column_index = column_letter_to_number(finish_column)

  return sheet_name, start_row - 1, finish_row - 1, start_column_index, finish_column_index


def get_spreadsheet_values(spreadsheet_instance, spreadsheet_id, range):
  get_values_request = spreadsheet_instance.values().get(spreadsheetId=spreadsheet_id, range=range)
  return get_values_request.execute().get('values')


def update_cell_values(spreadsheet_instance, spreadsheet_id, values, range, major_dimension='ROWS', input_option='RAW'):
    body = {
        'valueInputOption': input_option,
        'data': [
            {
                'range': range,
                'majorDimension': major_dimension,
                'values': values
            } 
        ],
        'includeValuesInResponse': False
    }
    return spreadsheet_instance.values().batchUpdate(spreadsheetId=spreadsheet_id, body=body).execute()
  

def append_cell_values(spreadsheet_instance, spreadsheet_id, values, range, major_dimension='ROWS', input_option='RAW'):
  body = {
    'majorDimension': major_dimension,
    'values': values           
  }
  return spreadsheet_instance.values().append(spreadsheetId=spreadsheet_id, range=range, valueInputOption=input_option, body=body).execute()
  
  
def clear_values(spreadsheet_instance, spreadsheet_id, range):
  return spreadsheet_instance.values().clear(spreadsheetId=spreadsheet_id, range=range).execute()


"""Functions for creating requesets for batch updating spreadsheet"""

def make_create_sheet_request(title, rows, columns):
    return {'addSheet': {
            'properties': {
                'title': title,
                'gridProperties': {
                    'rowCount': rows,
                    'columnCount': columns
                }
            }
        }
    }
    

def make_delete_sheet_request(sheet_id):
    return {'deleteSheet': {'sheetId': sheet_id}}


def make_freeze_dimensions_request(sheet_id, number_rows, number_columns):
  return {'updateSheetProperties': {
            'properties': {
                'sheetId': sheet_id,
                'gridProperties': {
                  'frozenRowCount': number_rows,
                  'frozenColumnCount': number_columns 
                }
            },
            'fields': 'gridProperties.frozenRowCount,gridProperties.frozenColumnCount'
        }
    }
    


def make_append_dimension_request(sheet_id, dimension, length):
    return {'appendDimension': {'sheetId': sheet_id, 'dimension': dimension, 'length': length}}


def make_delete_dimension_request(sheet_id, dimension, start, finish):
    return {'deleteDimension': {'range': {'sheetId': sheet_id, 'dimension': dimension, 'startIndex': start, 'endIndex': finish}}}


def make_update_cell_note_request(sheet_id, row, column, note):
    return {'updateCells': {
            'rows': [
                {'values': [{'note': note}]}
            ],
            'fields': 'note',
            'start': {'sheetId': sheet_id, 'rowIndex': row, 'columnIndex': column}
        }
    }
    

def make_conditional_format_columns_request(sheet_id, start_row, end_row, start_column, end_column):
  return {'addConditionalFormatRule': {
    'rule':{
        'ranges': [{'sheetId': sheet_id, 'startRowIndex': start_row, 'endRowIndex': end_row, 'startColumnIndex': start_column, 'endColumnIndex': end_column}],
         'gradientRule': {
              'minpoint': {
                  'color': {
                    'red': 208/255,
                    'green': 226/255,
                    'blue': 248/255
                  },
                'type': 'MIN'
              },
              "maxpoint": {
                  'color': {
                    'red': 33/255,
                    'green': 112/255,
                    'blue': 204/255
                  },
                'type': 'MAX'
              }
            }
          },
    'index': 0
        }
      }
  
  
def make_dimension_size_request(sheet_id, dimension, start_index, end_index, size):
  return {'updateDimensionProperties': {
      'range': {'sheetId': sheet_id, 'dimension': dimension, 'startIndex': start_index, 'endIndex': end_index},
      'properties': {'pixelSize': size},
      'fields': 'pixelSize'
    }
  }
  

def make_dimension_auto_resize_request(sheet_id, dimension, start, finish):
  return {'autoResizeDimensions': {
      'dimensions': {'sheetId': sheet_id, 'dimension': dimension, 'startIndex': start, 'endIndex': finish}
    }
  }


def make_merge_cells_request(sheet_id, start_row, end_row, start_column, end_column, merge_type='MERGE_ALL'):
  return {'mergeCells': {
      'range': {'sheetId': sheet_id, 'startRowIndex': start_row, 'endRowIndex': end_row, 'startColumnIndex': start_column, 'endColumnIndex': end_column},
      'mergeType': merge_type
    }
  }

  
def make_cells_alignment_request(sheet_id, start_row_index, end_row_index, start_column_index, end_column_index, vertical_alignment, horizontal_alignment, wrap_strategy):
  return {'repeatCell':{
      'cell': {
          'userEnteredFormat': {
            'horizontalAlignment': horizontal_alignment,
            'verticalAlignment': vertical_alignment,
            'wrapStrategy': wrap_strategy
          }
        },
      'range': {
        'sheetId': sheet_id,
        'startRowIndex': start_row_index,
        'endRowIndex': end_row_index,
        'startColumnIndex': start_column_index,
        'endColumnIndex': end_column_index
        },
      'fields': 'userEnteredFormat.horizontalAlignment,userEnteredFormat.verticalAlignment,userEnteredFormat.wrapStrategy'
    }
  }
  

def make_cell_borders_request(
    sheet_id,
    start_row_index,
    end_row_index,
    start_column_index,
    end_column_index,
    top_style='NONE',
    bottom_style='NONE',
    left_style='NONE',
    right_style='NONE',
    inner_vertical_style='NONE',
    inner_horizontal_style='NONE'
  ):
    return {'updateBorders':{
      'range': {
        'sheetId': sheet_id,
        'startRowIndex': start_row_index,
        'endRowIndex': end_row_index,
        'startColumnIndex': start_column_index,
        'endColumnIndex': end_column_index
        }, 
      'top': {'style': top_style},
      'bottom': {'style': bottom_style},
      'left': {'style': left_style},
      'right': {'style': right_style},
      'innerHorizontal': {'style': inner_horizontal_style},
      'innerVertical': {'style': inner_vertical_style}
      }
    }
  
  
def make_text_format_request(sheet_id, start_row_index, end_row_index, start_column_index, end_column_index, size, bold=False, italic=False, strikethrough=False, underline=False):
  return {'repeatCell':{
      'cell': {
          'userEnteredFormat': {
            'textFormat': {
              'fontSize': size,
              'bold': bold,
              'italic': italic,
              'strikethrough': strikethrough,
              'underline': underline                            
            }
          }
        },
      'range': {
        'sheetId': sheet_id,
        'startRowIndex': start_row_index,
        'endRowIndex': end_row_index,
        'startColumnIndex': start_column_index,
        'endColumnIndex': end_column_index
        },
      'fields': 'userEnteredFormat.textFormat'
    }
  }


"""Google Drive API functions"""

def create_drive_instance(credentials):
  return build('drive', 'v3', credentials=credentials)


def share_file_for_anyone(drive_instance, file_id, role):
  permission = {
    'type': 'anyone',
    'role': role
  }
  drive_instance.permissions().create(fileId = file_id, body=permission, fields='id').execute()


def delete_file(drive_instance, file_id):
  drive_instance.files().delete(fileId=file_id).execute()


def find_all_files(drive_instance):
  responce = drive_instance.files().list().execute()
  return responce.get('files')


def show_all_files(drive_instance):
  files = find_all_files(drive_instance)
  print(f'Найдено {len(files)} файлов:')
  for file in files:
    print('ID:', file.get('id'), 'Name:', file.get('name'))
    

def open_all_files(drive_instance):
  files = find_all_files(drive_instance)
  for file in files:
    webbrowser.open('https://docs.google.com/spreadsheets/d/' + file.get('id'))


def get_file_id_by_name(drive_instance, name):
  for file in find_all_files(drive_instance):
    if file.get('name') == name:
      return file.get('id')
  print(f'No file with name "{name}" found!')
  return None
