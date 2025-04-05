import csv, io, os
from datetime import datetime
import pandas as pd
from io import BytesIO

from pydrive2.auth import GoogleAuth
from pydrive2.drive import GoogleDrive
import gspread
import logger.logger as logger


'''
google_tasks.py

A module of utility functions to simplify calls to public libs 
for interfacng with google cloud apis.
* google_sheets:  https://github.com/burnash/gspread
* google_drive:  https://github.com/iterative/PyDrive2
'''


### PyDrive2 ###
# https://docs.iterative.ai/PyDrive2/quickstart/

def __auth_g_drive__(creds_file: str) -> GoogleDrive:
    """
    Google Drive service with a service account.
    note: for the service account to work, you need to share the folder or
    files with the service account email.

    Returns a pydrive2.auth.GoogleDrive obj ready to use
    """
    # Define the settings dict to use a service account
    # https://docs.iterative.ai/PyDrive2/oauth/#authentication-with-a-service-account
    settings = {
                "client_config_backend": "service",
                "service_config": {
                    "client_json_file_path": creds_file
                }
            }
    try:
        # Create instance of GoogleAuth
        gauth = GoogleAuth(settings=settings)
        gauth.ServiceAuth()
        drive = GoogleDrive(auth=gauth)
    except RuntimeError as err:
        msg = f'Error authenticating via pyDrive. \n{err}\n Did you configure in settings?  Are the creds valid?'
        logger.log(msg, error=True)
        raise Exception(msg)

    return drive

def list_files_by_gdrive_id(creds_file: str, parent_id:str) -> list:
    gdrive_client = __auth_g_drive__(creds_file)
    qry_str = f"'{parent_id}' in parents"
    file_list = gdrive_client.ListFile({'q': qry_str}).GetList()
    file_ids = [f['id'] for f in file_list]
    return file_ids

def get_gdrivefile_metadata(creds_file: str, file_id: str, metadata_field: str) -> any:
    '''
    returns gDrive metadata for a file by ID.
    https://docs.iterative.ai/PyDrive2/filemanagement/#download-file-metadata-from-file-id
    '''
    gdrive = __auth_g_drive__(creds_file)
    # Fetch metadata
    file = gdrive.CreateFile({'id': file_id})
    metadata = file[metadata_field]
    return metadata

def read_csv_from_gdrive(creds_file: str, file_id: str, title: str) -> list:
    '''
    return the contents of a .csv file as a list of row dicts [{col0:val, col1:val, ..}, {...}]
    '''
    # auth
    drive = __auth_g_drive__(creds_file)
    # Get the file data as a stream
    file = drive.CreateFile({'id': file_id})
    data = file.GetContentString(mimetype='text/csv', encoding='utf-8-sig')
    csv_file = io.StringIO(data)
    # return as a list of dicts
    reader = csv.DictReader(csv_file)
    data_rows = [r for r in reader]
    return data_rows

def read_xslx_from_gdrive(creds_file: str, file_id: str, title: str) -> list:
    '''
    return the contents of a .xlsx file as a list of row dicts [{col0:val, col1:val, ..}, {...}]
    '''
    # auth
    drive = __auth_g_drive__(creds_file)
    # Get the file data as a stream
    file = drive.CreateFile({'id': file_id})

    data = None
    with BytesIO() as buffer:
        for chunk in file.GetContentIOBuffer():
           buffer.write(chunk)
        data = buffer.getvalue()
    df = pd.read_excel(BytesIO(data))
    data_rows = df.to_dict(orient='records')
    return data_rows

def move_files_in_gdrive(creds_file: str, files: list, to_folder_id: str) -> None:
    ''' 
    Moves files that have been loaded into the DB from inbox to processed folder in GDrive 

    Also, inserts a timestamp in fname for forensics, eg. import.csv => import(timestamp).csv
    '''
    gdrive = __auth_g_drive__(creds_file)
   
    now_str = datetime.now().strftime('%Y-%m-%d-%H:%M')

    for f in files:
        file = gdrive.CreateFile({'id': f})
        tit = file['title']
        new_tit = tit[:-4] + f'({now_str})' + tit[-4:] 
        file['title'] = new_tit
        file['parents'] = [{'kind': 'drive#folder', 'id': to_folder_id}]
        file.Upload()
        logger.info(f'{tit} moved to processed folder as {new_tit}')
    return

def add_file_to_gdrive(creds_file:str, local_file: str, to_folder_id: str) -> None:

    # auth
    gdrive = __auth_g_drive__(creds_file)

    # file defs for gDrive
    local_f_basename = os.path.basename(local_file)
    metadata = {
        'parents': [
            {"id": to_folder_id}
        ],
        'title': f'{local_f_basename}'}

    # Find existing and delete
    file_list = gdrive.ListFile({'q': f"title='{local_f_basename}' and trashed=false and '{to_folder_id}' in parents"}).GetList()
    for file in file_list:
    # trash one if it exists in target folder then write the new one
        try:
            # thows 403 if the service user did not create the file
            file.Trash()
            # file.Delete()   # instant, permanent
        except:
            msg = f'error adding "{local_f_basename}" to GDrive folder {to_folder_id}'
            logger.error(msg)
    try:
        file = gdrive.CreateFile(metadata=metadata)
        file.SetContentFile(local_file)  
        file.Upload()
        logger.info(f'{local_file} uploaded to gDrive folder for safe keeping')
    except Exception as e:
        msg = f'error adding "{local_f_basename}" to GDrive folder {to_folder_id}\n\t{e}'
        print(msg)
    return

### gSpread ###
# https://docs.gspread.org/en/latest/user-guide.html

def __auth_g_sheets__(creds_file: str) -> gspread.service_account:
    '''
    Google Spreadsheet cient with a service account.
    Please note: 
    * for the service account be useful, you need to share gSheets with the service account email.
     
    Returns a gspread.Client obj ready to use
    '''
    # https://docs.gspread.org/en/latest/oauth2.html#for-bots-using-service-account
    try:
        gc = gspread.service_account(filename=creds_file)
    except RuntimeError as err:
        msg = f'gSpread auth failed. \n{err} \n Did you configure in settings?  Are the creds valid?'
        logger.ERROR(msg)
        raise Exception(msg)
    
    return gc

def read_gsheet_range(creds_file: str, gSpread_detail: dict) -> list:
    '''
    Returns data from a spreadsheet as a list of rows (lists)

    gSpread_detail should be a dict must contains these keys:
     * file_id
     * sheet_id
     * range
    '''
    # auth via gSpread and get a Client - https://docs.gspread.org/en/latest/api/client.html
    gspread_client = __auth_g_sheets__(creds_file)

    # where is the data to read?
    gsheet_id = gSpread_detail['file_id']
    wksht_id = gSpread_detail['sheet_id']
    rng = gSpread_detail['range']

    # read that sht
     # https://docs.gspread.org/en/latest/api/models/index.html
    try:
        gsheet = gspread_client.open_by_key(gsheet_id)
    except PermissionError:
        msg = "Did you grant the service user permissions on the GFrive files & folders? ccdg-google-service-account@ccdg-csv-utility.iam.gserviceaccount.com"
        print(msg)
    wksht = gsheet.get_worksheet_by_id(wksht_id)
    data = wksht.get(rng)
    
    return data

def write_gsheet_range(creds_file: str, gSpread_detail: dict, row_data: list) -> None:
    '''
    Writes a list of lists-as-rows to a gSheet range - each inner-list item is a cell in that row
    e.g. in A1 notation: [[A1,B1],[A2,B2],[A3,B3]] is a two-col list with three rows
    '''
   # auth via gSpread and get a Client - https://docs.gspread.org/en/latest/api/client.html
    gspread_client = __auth_g_sheets__(creds_file)

    # Write the list of rows to the Google Sheet
    # https://docs.gspread.org/en/latest/user-guide.html#updating-cells
    
    # where to write data
    gsheet_id = gSpread_detail['file_id']
    wksht_id = gSpread_detail['sheet_id']
    
    # open in gSpread
    gsheet = gspread_client.open_by_key(gsheet_id)
    wksht = gsheet.get_worksheet_by_id(wksht_id)
    
    # clear sheet & write all new rows
    wksht.clear()
    wksht.update('A1', row_data)

    return

def list_to_dict(data: list):
    """
    Converts a list of lists into a list of dictionaries using the first row as headers.

    Args:
        data (list): A list of lists where the first list contains headers.

    Returns:
        list of dict: A list of dictionaries with headers as keys.
    """
    if not data or len(data) < 2:
        return []  # Return empty list if data is empty or has no rows beyond headers
    
    headers = data[0]  # Extract column headers
    return [dict(zip(headers, row)) for row in data[1:]]  # Map headers to each row

def dicts_to_list(data: list):
    '''
    Converts a list of dictionaries into a list of lists using the keys of the first dictionary as headers.
    Args:
        data (list): A list of dictionaries.
    Returns:  
        list of lists: A list of lists with the first list as headers.
    '''
    if not data:
        return []  # Return empty list if input is empty
    
    # Extract headers from the keys of the first dictionary
    headers = list(data[0].keys())

    # Convert dicts to lists using the headers
    rows = [headers] + [[d[key] for key in headers] for d in data]

    return rows