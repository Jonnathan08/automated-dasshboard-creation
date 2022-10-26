import requests
from swagger_client.rest import ApiException
import swagger_client
import snowflake.connector
from pprint import pprint
from typing import Any, Dict
import os
import json
import pandas as pd
import csv
#from dotenv import load_dotenv
from pathlib import Path

# Compass API
def init_conn():
    #load_dotenv(str(Path.home() / 'compass_config/.env'))
    
    creds = csv.DictReader(open(r"credentials2.csv"),delimiter=',')

    for cred in creds:
        user_creds = cred
        
    os.environ['username'] = user_creds['username']
    os.environ['password'] = user_creds['password']
    os.environ['client_id'] = user_creds['client_id']
    os.environ['client_secret'] = user_creds['client_secret']
    
    
   
    verify_cec_credentials_are_set()
    
    access_token =  get_one_hour_access_token()
    
    configuration =  get_api_configuration(
            access_token=access_token)
    
    user_info =  get_user_info(
            configuration=configuration)
    
    return access_token,configuration,user_info

def verify_cec_credentials_are_set():
    cec_username = os.getenv('username', None)
    cec_password = os.getenv('password', None)

    if not cec_username or not cec_password:
        raise RuntimeError(
            'Your CEC Username and Password must be set with the cec_setter tool before running ' +
            '(Usage: ". ./cec_setter.sh").')
        
def get_one_hour_access_token() -> str:
    """
    Get an access token to use the request tracker APIs
    These tokens last for one hour
    """
    response = requests.post(
        url='https://cloudsso.cisco.com/as/token.oauth2',
        params={
            'grant_type': 'password',
            'client_id': os.getenv('client_id'),
            'client_secret': os.getenv('client_secret'),
            'username': os.getenv('username'),
            'password': os.getenv('password'),
        }
    )

    assert response.status_code == 200
    access_token = response.json()['access_token']
    return access_token


def get_api_configuration(access_token: str) -> swagger_client.Configuration:
    """
    Get the API configuration based on an access token
    :param access_token: A token retrieved to use with request tracker APIs
    """
    # Configure API key authorization: bearerAuthorization
    configuration = swagger_client.Configuration()
    configuration.api_key['Authorization'] = access_token
    configuration.api_key_prefix['Authorization'] = 'Bearer'
    return configuration


def get_user_info(configuration: swagger_client.Configuration) -> Dict[str, Any]:
    """
    Get the user information based on an access token
    :param configuration: API Configuration
    """
    print('\n\nget_user_info')
    api_instance = swagger_client.UserInfoApi(
        swagger_client.ApiClient(configuration))

    try:
        api_response = api_instance.get_user_info()
        return api_response
    except ApiException as e:
        print("Exception when calling UserInfoApi->get_user_info: %s\n" % e)


def create_request(configuration: swagger_client.Configuration) -> Dict[str, Any]:
    """
    Create a Compass request
    :param configuration: API Configuration
    """
    print('\n\ncreate_request')
    api_instance = swagger_client.RequestsApi(
        swagger_client.ApiClient(configuration))

    body = {
        "accountName": "Python",
        "requesterName": "requester@cisco.com",
        "requesterFunction": "BDM",
        "dealId": "12345678",
        "savId": "123456789,234567890",
        "guId": "12345678"
    }

    try:
        api_response = api_instance.create_request(body)
        pprint(api_response)
        return api_response
    except ApiException as e:
        print("Exception when calling RequestsApi->create_request: %s\n" % e)


def get_request(configuration: swagger_client.Configuration, request_id: int) -> Dict[str, Any]:
    """
    Get a Compass request
    :param configuration: API Configuration
    """
    print('\n\nget_request')
    api_instance = swagger_client.RequestsApi(
        swagger_client.ApiClient(configuration))

    try:
        api_response = api_instance.get_request(request_id)
        pprint(api_response)
        return api_response
    except ApiException as e:
        print("Exception when calling RequestsApi->get_request: %s\n" % e)

def get_requests2(configuration: swagger_client.Configuration,q) -> Dict[str, Any]:
    """
    Get Compass requests using a query
    :param configuration: API Configuration
    """
    api_instance = swagger_client.RequestsApi(
        swagger_client.ApiClient(configuration))
    
    try:
        api_response = api_instance.get_requests(q=json.dumps(q),max=1000,sort='requestedDtm',order='desc')
        return api_response
    except ApiException as e:
        print("Exception when calling RequestsApi->get_requests: %s\n" % e)
    
def patch_request(configuration: swagger_client.Configuration, request_id: int, body) -> Dict[str, Any]:
    """
    Update a Compass request
    :param configuration: API Configuration
    """
    api_instance = swagger_client.RequestsApi(
        swagger_client.ApiClient(configuration))

    try:
        api_response = api_instance.patch_request(body, request_id)
        return api_response 
    except ApiException as e:
        print("Exception when calling RequestsApi->patch_request: %s\n" % e)

def get_ids_list(fields_df,separator=','):
    sav_l = fields_df.query("`ID TYPE` == 'SAV ID'")["SAV ID"].tolist()
    
    sav_str_list = separator.join(sav_l)

    gu_l = fields_df.query("`ID TYPE` == 'GU ID'")["GU ID"].tolist()
    
    gu_str_list = separator.join(gu_l)
    
    cav_l = fields_df.query("`ID TYPE` == 'CAV ID'")["CAV ID"].tolist()
    
    cav_str_list = separator.join(cav_l)

    cr_l = fields_df.query("`ID TYPE` == 'CR Party ID'")["CR Party ID"].tolist()
    
    cr_str_list = separator.join(cr_l)
    return sav_str_list,gu_str_list,cav_str_list,cr_str_list

def get_uncovered_data2_rows(user,ids_sav,ids_gu,ids_cr,ids_cav):
    
    """Get uncovered data from IDs
    
    param: user - cisco e-mail address
    param: ids - list of given account ids"""
     
    cnn = snowflake.connector.connect(
    user=user,
    authenticator='externalbrowser',
    role='CX_CA_BUS_ANALYST_ROLE',
    warehouse='CX_CA_RPT_WH',
    database='CX_DB',
    schema='CX_CA_BR',
    account='cisco.us-east-1'
    )
    
    cs = cnn.cursor()
    
    dfs = []
    types_list = {'SAV':ids_sav,'GU':ids_gu,'CR':ids_cr,'CAV':ids_cav}
    
    for type_id in types_list.keys():

        if types_list.get(type_id) == '': pass
        else:

            query_uncovered = f"""SELECT CUSTOMER_ID,ACCOUNT_IDENTIFIER, COUNT(1) FROM "CX_DB"."CX_CA_BR"."BV_CX_IB_COLLECTOR_FINAL"
                                WHERE CUSTOMER_ID IN ({types_list.get(type_id)}) AND ACCOUNT_IDENTIFIER = '{type_id}'
                                GROUP BY CUSTOMER_ID,ACCOUNT_IDENTIFIER"""

            cs.execute(query_uncovered)
            df = cs.fetchall()

            uncovered_columns = ["CUSTOMER_ID","ACCOUNT_IDENTIFIER","NUM_ROWS_UNCOVERED"]

            df = pd.DataFrame(df,columns=uncovered_columns)
            dfs.append(df)

    uncovered_df = pd.concat(dfs)
    
    #types = uncovered_df.dtypes.to_dict()
    
    return uncovered_df

def get_appliance_data_rows(user,ids_sav,ids_gu,ids_cr,ids_cav):
    
    """Get appliance details data from IDs
    
    param: user - cisco e-mail address
    param: ids - list of given account ids"""
     
    cnn = snowflake.connector.connect(
    user=user,
    authenticator='externalbrowser',
    role='CX_CA_BUS_ANALYST_ROLE',
    warehouse='CX_CA_RPT_WH',
    database='CX_DB',
    schema='CX_CA_BR',
    account='cisco.us-east-1'
    )
    
    cs = cnn.cursor()
    
    dfs = []
    types_list = {'SAV':ids_sav,'GU':ids_gu,'CR':ids_cr,'CAV':ids_cav}
    
    for type_id in types_list.keys():

        if types_list.get(type_id) == '': pass
        else:

            query_appliance = f"""SELECT CUSTOMER_ID,CUSTOMER_IDENTIFIER, COUNT(1) FROM "CX_DB"."CX_CA_BR"."BV_CX_COLLECTOR_APPLIANCE_DETAILS"
                                WHERE CUSTOMER_ID IN ({types_list.get(type_id)}) AND CUSTOMER_IDENTIFIER = '{type_id}'
                                GROUP BY CUSTOMER_ID,CUSTOMER_IDENTIFIER"""

            cs.execute(query_appliance)
            df = cs.fetchall()

            appliance_columns = ["CUSTOMER_ID","CUSTOMER_IDENTIFIER",'NUM_ROWS_APPLIANCE']

            df = pd.DataFrame(df,columns=appliance_columns)
            dfs.append(df)

    appliance_df = pd.concat(dfs)
    
    #types = uncovered_df.dtypes.to_dict()
    
    return appliance_df

def get_coverage_data_rows(user,ids_sav,ids_gu,ids_cr,ids_cav):
    
    """Get coverage data from IDs
    
    param: user - cisco e-mail address
    param: ids - list of given account ids"""
     
    cnn = snowflake.connector.connect(
    user=user,
    authenticator='externalbrowser',
    role='CX_CA_BUS_ANALYST_ROLE',
    warehouse='CX_CA_RPT_WH',
    database='CX_DB',
    schema='CX_CA_BR',
    account='cisco.us-east-1'
    )
    
    cs = cnn.cursor()
    
    dfs = []
    types_list = {'SAV':ids_sav,'GU':ids_gu,'CR':ids_cr,'CAV':ids_cav}
    
    for type_id in types_list.keys():

        if types_list.get(type_id) == '': pass
        else:

            query_coverage = f"""SELECT CUSTOMER_ID, ACCOUNT_IDENTIFIER, COUNT(1) 
                                 FROM "CX_DB"."CX_CA_BR"."BV_CX_IB_ASSET_VW"
                                 WHERE CUSTOMER_ID IN ({types_list.get(type_id)}) AND ACCOUNT_IDENTIFIER = '{type_id}'
                                 GROUP BY CUSTOMER_ID, ACCOUNT_IDENTIFIER """

            cs.execute(query_coverage)
            df = cs.fetchall()

            coverage_columns = ["CUSTOMER_ID","ACCOUNT_IDENTIFIER","NUM_ROWS_COVERAGE"]

            df = pd.DataFrame(df,columns=coverage_columns)
            dfs.append(df)

    coverage_df = pd.concat(dfs)
    
    #types = coverage_df.dtypes.to_dict()
    
    return coverage_df

def get_contracts_data_rows(user,ids_sav,ids_gu,ids_cr,ids_cav):
    
    """Get contracts data from IDs
    
    param: user - cisco e-mail address
    param: ids - list of given account ids"""
    
    cnn = snowflake.connector.connect(
    user=user,
    authenticator='externalbrowser',
    role='CX_CA_BUS_ANALYST_ROLE',
    warehouse='CX_CA_RPT_WH',
    database='CX_DB',
    schema='CX_CA_BR',
    account='cisco.us-east-1'
    )
    
    cs = cnn.cursor()
    
    dfs = []
    types_list = {'SAV':ids_sav,'GU':ids_gu,'CR':ids_cr,'CAV':ids_cav}
    
    for type_id in types_list.keys():

        if types_list.get(type_id) == '': pass
        else:

            query_contracts = f"""SELECT CUSTOMER_ID, ACCOUNT_IDENTIFIER, COUNT(1)
                                  FROM "CX_DB"."CX_CA_BR"."BV_CX_IB_ASSET_CONTRACT_VW"
                                  WHERE CUSTOMER_ID IN ({types_list.get(type_id)}) AND ACCOUNT_IDENTIFIER = '{type_id}'
                                  GROUP BY CUSTOMER_ID, ACCOUNT_IDENTIFIER """

            cs.execute(query_contracts)
            df = cs.fetchall()

            contracts_columns = ["CUSTOMER_ID","ACCOUNT_IDENTIFIER","NUM_ROWS_CONTRACTS"]

            df = pd.DataFrame(df,columns=contracts_columns)
            dfs.append(df)

    contracts_df = pd.concat(dfs)
    
    #types = contracts_df.dtypes.to_dict()
    
    return contracts_df

def get_tac_data_rows(user,ids_sav,ids_gu,ids_cr,ids_cav): 
    
    """Get TAC data from Snowflake by given IDs and
    creates a DataFrame
    
    param: user - cisco e-mail address
    param: ids"""
    
    cnn = snowflake.connector.connect(
    user=user,
    authenticator='externalbrowser',
    role='CX_CA_BUS_ANALYST_ROLE',
    warehouse='CX_CA_RPT_WH',
    database='CX_DB',
    schema='CX_CA_BR',
    account='cisco.us-east-1'
    )
    
    cs = cnn.cursor()
    
    dfs = []
    types_list = {'SAV':ids_sav,'GU':ids_gu,'CR':ids_cr,'CAV':ids_cav}
    
    for type_id in types_list.keys():

        if types_list.get(type_id) == '': pass
        else:
            
            if type_id == 'CR':
                
                query_tac = f"""SELECT "PARTY ID",FLAG, COUNT(1) FROM "CX_DB"."CX_CA_BR"."TAC_UNION" 
                    WHERE FLAG = 'GU' AND "PARTY ID" in ({types_list.get(type_id)})
                    GROUP BY FLAG, "PARTY ID" """
            else:
                query_tac = f"""SELECT ID,FLAG,COUNT(1) FROM "CX_DB"."CX_CA_BR"."TAC_UNION" 
                    WHERE FLAG = '{type_id}' AND ID in ({types_list.get(type_id)})
                    GROUP BY ID,FLAG"""
                
            cs.execute(query_tac)
            df = cs.fetchall()

            tac_columns = ['CUSTOMER_ID','ACCOUNT_IDENTIFIER','NUM_ROWS_TAC']

            df = pd.DataFrame(df,columns=tac_columns)
            dfs.append(df)

    tac_df = pd.concat(dfs)
    
    return tac_df