from __future__ import print_function
import pandas as pd
import random
import datetime
import numpy as np  

class bcolors:
    HEADER = '\033[95m'
    OKBLUE = '\033[94m'
    OKCYAN = '\033[96m'
    OKGREEN = '\033[92m'
    WARNING = '\033[93m'
    FAIL = '\033[91m'
    ENDC = '\033[0m'
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'

try:
    import smartsheet
except AttributeError as e:
    print(bcolors.FAIL + bcolors.BOLD + 'Error importing Smartsheet lib: ' + bcolors.ENDC + "Try to change the python version, Eg. 'Python 3.8.8'\n")
    raise

def init_conn(token):
    SMARTSHEET_ACCESS_TOKEN = token #"Jfm6LbqQyQoC63oFfzEN0cTiuHJKh0lUwXilp"
    # Initialize client
    smartsheet_client = smartsheet.Smartsheet(SMARTSHEET_ACCESS_TOKEN)
    return smartsheet_client

def load_sheet(sheet_id,client,modified_since=None):
    # load DA Tracker Sheet
    
    oa_sheet = client.Sheets.get_sheet(sheet_id,rows_modified_since=modified_since)
    
    print("Loaded Sheet: " + oa_sheet.name)
    print("Rows: " + str(len(oa_sheet.rows)))
    return oa_sheet
    
def map_columns(sheet):
    """Map columns with their respective smartsheet column ids
    {
    column name: Smartsheet column id
    }
    """
    
    column_map ={}
    for column in sheet.columns:
     #Map Values
     column_map[column.title] = column.id
    return column_map

def map_columns_type(sheet):
    """Map columns with their respective smartsheet column types
    {
    column name: Smartsheet column type
    }
    """
    
    column_map ={}
    for column in sheet.columns:
     #Map Values
     column_map[column.title] = column.to_dict()["type"]
    return column_map

def map_rows(sheet):
    """Map rows with their respective smartsheet rows ids
    {
    row number: Smartsheet row id
    }
    """
    
    row_map = {}
    i=0 #counter
    for rows in sheet.rows:
     #Map Row ID
     row_map[i]=rows.id
     i=i+1
    return row_map

def request_row_map(sheet,n):
    """Maps last n request ids to their respective row ids
    {
    Request ID: Smartsheet row id
    }
    """
    
    row_map = {}

    for rows in sheet.rows[-n:]:
        row_map[str(rows.to_dict()['cells'][0]['value']).split('.')[0]] = rows.id

    return row_map

def sheet_to_df2(sheet,columns,campaign=None):
    """Smartsheet sheet to pandas DataFrame"""
    #print(columns)
    rows_l = []
    for n in range(len(sheet)):
        row_cells = sheet[n].to_dict()['cells']
        row_n = {columns[i].to_dict().get('title',''): row_cells[i].get('value','') for i in range(len(row_cells))}
            
        rows_l.append(row_n)
    
    ss_df = pd.DataFrame.from_dict(rows_l)

    return ss_df 

def get_last_n_rows(sheet,n_rows=100):
    """Get last n records from sheet"""
    
    sheet_list = []

    for row in sheet.rows[-n_rows:]:
        sheet_list.append(row)
        
    return sheet_list

def assign_da_list(df,da_counts):
    """Assign DA to request"""
    
    DA2 = ['saescoba@cisco.com', 'malondon@cisco.com', 'camruiz@cisco.com', 'josbirch@cisco.com', 'josguti2@cisco.com', 'stotero@cisco.com']

    das = []

    max_n = 20 # max number of accounts to be assigned per DA

    for i in df.index:
        random.shuffle(DA2)
        for da in DA2:
            
            if (df['Lvl2 (Region)'][i] == 'US PS Market Segment'): # USPS DA restriction
                if da_counts.get('josbirch@cisco.com',0) <= max_n:
                    das.append('josbirch@cisco.com')
                    da_counts['josbirch@cisco.com'] = da_counts.get('josbirch@cisco.com',0) + 1
                    break
                elif da_counts.get('josbirch@cisco.com',0) > max_n:
                    das.append('')
                    break
            else:
                pass

            if (da_counts.get(da,0) <= max_n) and (df['Lvl2 (Region)'][i] != 'US PS Market Segment'):
                das.append(da)
                da_counts[da] = da_counts.get(da,0) + 1
                break

            if (all(x > max_n for x in da_counts.values()) == True): # when all DAs reach max number of accounts, remaining accounts to be assigned to Facundo
                das.append('')
                break

    return das

def update_sheet(smartsheet_client,sheet,sheet_df,oa_df):
    """Update OA Smartsheet with rows pulled from the compass requests tracker"""
    
    sheet_dict = sheet_df.to_dict(orient='records')

    column_map = map_columns(sheet)
    sheet_id = sheet.id
    
    for row in sheet_dict:

        if (str(row['Request ID']) not in oa_df['Request ID'].unique()):
            new_row = smartsheet.models.Row()
            new_row.toBottom=True 
            
            for field in sheet_dict[0].keys():
                new_value = ''
                new_value = row.get(field,'')
                
                if (new_value is not None) and (new_value != '') and (pd.isnull(new_value) == False):
                    #new_value = ''

                    row_body = {
                        'column_id': column_map[field],
                        'value': str(new_value)
                    }
                    
                    new_row.cells.append(row_body)

            try:
                response = smartsheet_client.Sheets.add_rows(sheet_id,[new_row])
            except:
                pass
            
def update_field_row_id2(smartsheet_client,sheet,row_id: int, fields, oa_df_row):
    """Update Smartsheet cell by row id"""
    
    date = datetime.date.today()
    now = datetime.datetime.now()
    current_time = now.strftime("%H:%M:%S")
    current_datetime = str(date) + ' ' + str(current_time)
    
    sheet_id = sheet.id
    column_map = map_columns(sheet)
    
    get_row = smartsheet.models.Row()
    
    for field in fields:
        
        if field == 'Uploaded Time':
            new_value = current_datetime
        else:
            new_value = oa_df_row[field]
        
    #Build the cell you are updating with
        new_cell = smartsheet_client.models.Cell()

        if new_value is None:
            new_value = ''

        #Update column Remaining
        new_cell.column_id = column_map[field]
        new_cell.value = new_value
        new_cell.strict = False

        # Build the row to update
        get_row.id = row_id
        get_row.cells.append(new_cell)

    updated_row = smartsheet_client.Sheets.update_rows(sheet_id,[get_row])