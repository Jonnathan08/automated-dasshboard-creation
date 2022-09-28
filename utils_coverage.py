import warnings
import os
import shutil
import snowflake.connector
import pandas as pd
import numpy as np
import itertools
from datetime import datetime
from snowflake.connector.pandas_tools import write_pandas

from pathlib import Path
from sqlalchemy import true

from tableauhyperapi import HyperProcess, Telemetry, \
    Connection, CreateMode, \
    NOT_NULLABLE, NULLABLE, SqlType, TableDefinition, \
    Inserter, \
    escape_name, escape_string_literal, \
    HyperException

warnings.filterwarnings("ignore")

def clean_region(region):
    region = region.replace('_','')
    region = region.split('-')
    
    return region[0]



def get_da_requests(da,df):
    oa_df = df.query("`Cov Assigned DA` == r'{}' and `Cov Status` == 'In Process'".format(da),
                                                           engine='python')
    
    fields_df = oa_df.copy()
    fields_df["SAV ID"] = fields_df["SAV ID"].apply(lambda x : str(x).replace(" ",'').split('.')[0])
    fields_df["GU ID"] = fields_df["GU ID"].apply(lambda x :  str(x).replace(" ",'').split('.')[0])
    fields_df["CAV ID"] = fields_df["CAV ID"].apply(lambda x : str(x).replace(" ",'').split('.')[0])
    fields_df["CR Party ID"] = fields_df["CR Party ID"].apply(lambda x :  str(x).replace(" ",'').split('.')[0])
    fields_df["Contract ID"] = fields_df["Contract ID"].apply(lambda x :  str(x).replace(" ",'').split('.')[0])
    fields_df["sav_list"] = fields_df["SAV ID"].apply(lambda x : x.split(','))
    fields_df["gu_list"] = fields_df["GU ID"].apply(lambda x : x.split(','))
    fields_df["cav_list"] = fields_df["CAV ID"].apply(lambda x : x.split(','))
    fields_df["cr_list"] = fields_df["CR Party ID"].apply(lambda x : x.split(','))
    fields_df["contract_list"] = fields_df["Contract ID"].apply(lambda x : x.split(','))
    fields_df["Lvl1"] = fields_df["Lvl1"].apply(lambda x : clean_region(x))
    fields_df["Date Created"] = pd.to_datetime(fields_df["Date Created"])
    fields_df.reset_index(inplace=True)
    
    return fields_df


def get_ids_list(fields_df,separator=';'):
    sav_l = fields_df.query("`ID TYPE` == 'SAV ID'")["sav_list"].tolist()
    flat_sav = list(itertools.chain(*sav_l))
    sav_str_list = separator.join(flat_sav)

    gu_l = fields_df.query("`ID TYPE` == 'GU ID'")["gu_list"].tolist()
    flat_gu = list(itertools.chain(*gu_l))
    gu_str_list = separator.join(flat_gu)
    
    cav_l = fields_df.query("`ID TYPE` == 'CAV ID'")["cav_list"].tolist()
    flat_cav = list(itertools.chain(*cav_l))
    cav_str_list = separator.join(flat_cav)

    cr_l = fields_df.query("`ID TYPE` == 'CR Party ID'")["cr_list"].tolist()
    flat_cr = list(itertools.chain(*cr_l))
    cr_str_list = separator.join(flat_cr)
    
    return sav_str_list,gu_str_list,cav_str_list,cr_str_list

def get_uncovered_data(user,ids_sav,ids_gu,ids_cr,ids_cav):
    
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

            query_uncovered = f"""SELECT * FROM "CX_DB"."CX_CA_BR"."BV_CX_IB_COLLECTOR_FINAL"
                                WHERE CUSTOMER_ID IN ({types_list.get(type_id)}) AND ACCOUNT_IDENTIFIER = '{type_id}'"""

            cs.execute(query_uncovered)
            df = cs.fetchall()

            uncovered_columns = ["CUSTOMER_ID","CUSTOMER_NAME","ACCOUNT_IDENTIFIER","L1_SALES_TERRITORY_DESCR",
                                 "L2_SALES_TERRITORY_DESCR","COVERAGE","SERVICE_CONTRACT_NUMBER","CONTRACT_LINE_STATUS",
                                 "SERIAL_NUMBER","INSTANCE_NUMBER","INSTANCE_STATUS","PRODUCT_SALES_ORDER_NUMBER",
                                 "CONTRACT_LINE_END_DATE","CONTRACT_LINE_END_FISCAL_QUARTER","CONTRACT_LINE_END_FISCAL_YEAR",
                                 "SHIP_DATE","SHIP_MONTH_AGE","SHIPPED_FISCAL_YEAR","BK_PRODUCT_ID","PRODUCT_CATEGORY_CD",
                                 "DV_GOODS_PRODUCT_CATEGORY_CD","BK_PRODUCT_TYPE_ID","RU_BK_PRODUCT_FAMILY_ID","COMPONENT_TYPE",
                                 "CONTRACT_TYPE","SUB_BUSINESS_ENTITY_DESCR","BUSINESS_ENTITY_DESCR","LAST_SUPPORT_DT",
                                 "LDOS_AGE","LDOS_FISCAL_YEAR","WARRANTY_TYPE","INSTALLATION_QUANTITY",
                                 "LATEST_QUALIFICATION_STATUS","BILLTO_CUSTOMER_NAME","PF_BAND","BASE_PRICE_USD_AMT",
                                 "PRODUCT_UNIT_LIST_PRICE","SNT","C2P","C4P","C4S","CS","SNC","SNTE","SNTP","ECMU","S2P",
                                 "ECMUS","SSC2P","SSC4P","SSC4S","SSCS","SSNCO","SSSNC","SSSNE","SSSNP","SSSNT","5SNTP",
                                 "REFRESH_DATE","SOURCE","EQUIPMENT_TYPE_DESCRIPTION","APPLIANCE_ID","INVENTORY",
                                 "COLLECTION_DATE","HOSTNAME","IPADDRESS","IMPORTED_BY","ALERT_URL","SERVICE_PROGRAM",
                                 "CONTRACT_END_DATE","EQUIPMENT_TYPE","UPDATED_DATE","ACCOUNT_ID",
                                 "CONTRACT_LINE_STATUS_FROM_IB","LINE_STATUS","SSPT_YORN","SNTC_YORN" ]

            df = pd.DataFrame(df,columns=uncovered_columns)
            dfs.append(df)

    uncovered_df = pd.concat(dfs)
    
    #types = uncovered_df.dtypes.to_dict()
    
    return uncovered_df

def get_uncovered_data2(user,ids_sav,ids_gu,ids_cr,ids_cav):
    
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

            query_uncovered = f"""SELECT * FROM "CX_DB"."CX_CA_BR"."BV_CX_IB_COLLECTOR_FINAL"
                                WHERE CUSTOMER_ID IN ({types_list.get(type_id)}) AND ACCOUNT_IDENTIFIER = '{type_id}'"""

            cs.execute(query_uncovered)
            df = cs.fetchall()

            uncovered_columns = ["CUSTOMER_ID","CUSTOMER_NAME","ACCOUNT_IDENTIFIER","L1_SALES_TERRITORY_DESCR",
                                 "L2_SALES_TERRITORY_DESCR","COVERAGE","SERVICE_CONTRACT_NUMBER","CONTRACT_LINE_STATUS",
                                 "SERIAL_NUMBER","INSTANCE_NUMBER","INSTANCE_STATUS","PRODUCT_SALES_ORDER_NUMBER",
                                 "CONTRACT_LINE_END_DATE","CONTRACT_LINE_END_FISCAL_QUARTER","CONTRACT_LINE_END_FISCAL_YEAR",
                                 "SHIP_DATE","SHIP_MONTH_AGE","SHIPPED_FISCAL_YEAR","BK_PRODUCT_ID","PRODUCT_CATEGORY_CD",
                                 "DV_GOODS_PRODUCT_CATEGORY_CD","BK_PRODUCT_TYPE_ID","DV_ITEM_TYPE_CD","RU_BK_PRODUCT_FAMILY_ID",
                                 "COMPONENT_TYPE","CONTRACT_TYPE","SUB_BUSINESS_ENTITY_DESCR",
                                 "BUSINESS_ENTITY_DESCR","LAST_SUPPORT_DT",
                                 "LDOS_AGE","LDOS_FISCAL_YEAR","WARRANTY_TYPE","INSTALLATION_QUANTITY",
                                 "LATEST_QUALIFICATION_STATUS","BILLTO_CUSTOMER_NAME","PF_BAND","BK_BE_GEO_ID_INT",
                                 "CHANNEL_PARTNER_NAME","INSTALL_SITE_ID","COUNTRY_NAME",
                                 "BASE_PRICE_USD_AMT",
                                 "PRODUCT_UNIT_LIST_PRICE","SNT","C2P","C4P","C4S","CS","SNC","SNTE","SNTP","ECMU","S2P",
                                 "ECMUS","SSC2P","SSC4P","SSC4S","SSCS","SSNCO","SSSNC","SSSNE","SSSNP","SSSNT","5SNTP",
                                 "REFRESH_DATE","SOURCE","EQUIPMENT_TYPE_DESCRIPTION","APPLIANCE_ID","INVENTORY",
                                 "COLLECTION_DATE","HOSTNAME","IPADDRESS","IMPORTED_BY","ALERT_URL","SERVICE_PROGRAM",
                                 "CONTRACT_END_DATE","EQUIPMENT_TYPE","UPDATED_DATE","ACCOUNT_ID",
                                 "CONTRACT_LINE_STATUS_FROM_IB","LINE_STATUS","SSPT_YORN","SNTC_YORN" ]

            df = pd.DataFrame(df,columns=uncovered_columns)
            dfs.append(df)

    uncovered_df = pd.concat(dfs)
    
    #types = uncovered_df.dtypes.to_dict()
    
    return uncovered_df


def get_coverage_data(user,ids_sav,ids_gu,ids_cr,ids_cav):
    
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

            query_coverage = f"""SELECT * FROM "CX_DB"."CX_CA_BR"."BV_CX_IB_ASSET_VW"
                                WHERE CUSTOMER_ID IN ({types_list.get(type_id)}) AND ACCOUNT_IDENTIFIER = '{type_id}'"""

            cs.execute(query_coverage)
            df = cs.fetchall()

            coverage_columns = ["CUSTOMER_ID","CUSTOMER_NAME","ACCOUNT_IDENTIFIER","BUSINESS_ENTITY_DESCR",
                        "RU_BK_PRODUCT_FAMILY_ID","BK_PRODUCT_ID","COVERED_ITEM_QTY","SERVICE_LIST_PRICE",
                        "ANNUALIZED_EXTENDED_CONTRACT_LINE_LIST_USD_AMOUNT","UNCOVERED_ITEM_QTY","UNCOVERED_SNTC_LIST_PRICE",
                        "UNCOVERED_SSPT_LIST_PRICE","PRODUCT_UNIT_LIST_PRICE"
                        ]

            df = pd.DataFrame(df,columns=coverage_columns)
            dfs.append(df)

    coverage_df = pd.concat(dfs)
    
    #types = coverage_df.dtypes.to_dict()
    
    return coverage_df

def get_contracts_data(user,ids_sav,ids_gu,ids_cr,ids_cav):
    
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

            query_contracts = f"""SELECT * FROM "CX_DB"."CX_CA_BR"."BV_CX_IB_ASSET_CONTRACT_VW"
                                WHERE CUSTOMER_ID IN ({types_list.get(type_id)}) AND ACCOUNT_IDENTIFIER = '{type_id}'"""

            cs.execute(query_contracts)
            df = cs.fetchall()

            contracts_columns = ["CUSTOMER_ID","CUSTOMER_NAME","ACCOUNT_IDENTIFIER","BUSINESS_ENTITY_DESCR","BK_PRODUCT_ID",
                         "CONTRACT_TYPE","CONTRACT_LINE_END_FISCAL_QUARTER","SNTC_SSPT_OFFER_FLAG","COVERED_ITEM_QTY",
                         "ANNUALIZED_EXTENDED_CONTRACT_LINE_LIST_USD_AMOUNT","ANNUALIZED_CONTRACT_LINE_NET_USD_AMOUNT",
                         "PRODUCT_UNIT_LIST_PRICE","SERVICE_LIST_PRICE","CORRECTED_CONTRACT_LINE_NET_USD_AMOUNT"]

            df = pd.DataFrame(df,columns=contracts_columns)
            dfs.append(df)

    contracts_df = pd.concat(dfs)
    
    #types = contracts_df.dtypes.to_dict()
    
    return contracts_df


def get_tac_data(user,ids_sav,ids_gu,ids_cr,ids_cav): 
    
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
                
                query_tac = f"""SELECT * FROM "CX_DB"."CX_CA_BR"."TAC_UNION" 
                    WHERE FLAG = 'GU' AND "PARTY ID" in ({types_list.get(type_id)})"""
            else:
                query_tac = f"""SELECT * FROM "CX_DB"."CX_CA_BR"."TAC_UNION" 
                    WHERE FLAG = '{type_id}' AND ID in ({types_list.get(type_id)})"""
                
            cs.execute(query_tac)
            df = cs.fetchall()

            tac_columns = ['INCIDENT_ID', 'INCIDENT_NUMBER', 'CURRENT_SEVERITY_INT',
                   'MAX_SEVERITY_INT', 'INCDT_CREATION_DATE',
                   'INCDT_CRET_FISCAL_WEEK_NUM_INT', 'SR Creation FY Week',
                   'SR Creation FY Month', 'SR Creation FY Quarter',
                   'SR Creation Fiscal Year', 'CLOSED_DATE',
                   'INCDT_CLOSED_FISCAL_MONTH_NM', 'INCDT_CLOSED_FISCAL_QTR_NM',
                   'INCDT_CLOSED_FISCAL_YEAR', 'SR_TIME_TO_CLOSE_DAYS_CNT',
                   'CURRENT_SERIAL_NUMBER', 'COMPLEXITY_DESCR', 'INITIAL_PROBLEM_CODE',
                   'OUTAGE_FLAG', 'ENTRY_CHANNEL_NAME', 'REQUEST_TYPE_NAME', 'BUG_CNT',
                   'INCIDENT_STATUS',
                   'CURRENT_ENTITLEMENT_TYPE_NAME', 'INCIDENT_CONTRACT_STATUS',
                   'CONTRACT NUMBER', 'CONTRACT_TYPE', 'CREATION_CONTRACT_SVC_LINE_ID',
                   'HW_VERSION_ID', 'SW_VERSION_ID',
                   'TAC_PRODUCT_SW_KEY', 'UPDATED_COT_TECH_KEY', 'INCIDENT_TYPE',
                   'PROBLEM_CODE', 'RESOLUTION_CODE', 'PART_NUMBER', 'SOLUTION_CNT',
                   'ERP_FAMILY', 'ERP_PLATFORM_NAME', 'TAC_PRODUCT_HW_KEY',
                   'TAC_HW_PLATFORM_NAME', 'TECH_NAME', 'BUSINESS_UNIT_CODE',
                   'HYBRID_PRODUCT_FAMILY', 'SUB_TECH_NAME', 'SR_PRODUCT_ID',
                   'UNDERLYING_CAUSE_NAME', 'UNDERLYING_CAUSE_DESCRIPTION', 'CASE_NUMBER',
                   'B2B_FLAG', 'TECH_ID', 'SUB_TECH_ID', 'SW_VERSION_ACT_ID',
                   'SW_VERSION_NAME', 'VALID_SR_FILTER_FLAG', 'CUSTOMER_VERTICAL_CD',
                   'CUSTOMER_MARKET_SEGMENT_CD', 'ISO_COUNTRY_CD',
                   'Initial Time to Resolution', 'Final Time to Resolution', 'SRC_DEL_FLG',
                   'Business Ownership Time', 'Customer Ownership Time',
                   'Other Ownership Time', 'Delivery Ownership Time', 'CUSTOMER_NAME',
                   'PARTY ID', 'PARTY NAME', 'ID', 'SERVICE_SUBGROUP_DESC',
                   'SERVICE_LEVLE_CODE', 'SERVICE_PROGRAM', 'SERVICE_BRAND',
                   'SR_TECHNOLOGY', 'SR_SUB_TECHNOLOGY', 'BE_INT', 'SUB_BE_INT', 'FLAG',
                   'Data Extracted Date', 'RU_BK_PRODUCT_FAMILY_ID']

            df = pd.DataFrame(df,columns=tac_columns)
            print('Creado')
            dfs.append(df)

    tac_df = pd.concat(dfs)
        
    # CURRENT_OWNER_CCO_ID
    # INCIDENT_CONTACT_EMAIL  ## Deleted from the view made by Facundo

    tac_columns = ['INCIDENT_ID', 'INCIDENT_NUMBER', 'CURRENT_SEVERITY_INT',
       'MAX_SEVERITY_INT', 'INCDT_CREATION_DATE',
       'INCDT_CRET_FISCAL_WEEK_NUM_INT', 'SR Creation FY Week',
       'SR Creation FY Month', 'SR Creation FY Quarter',
       'SR Creation Fiscal Year', 'CLOSED_DATE',
       'INCDT_CLOSED_FISCAL_MONTH_NM', 'INCDT_CLOSED_FISCAL_QTR_NM',
       'INCDT_CLOSED_FISCAL_YEAR', 'SR_TIME_TO_CLOSE_DAYS_CNT',
       'CURRENT_SERIAL_NUMBER', 'COMPLEXITY_DESCR', 'INITIAL_PROBLEM_CODE',
       'OUTAGE_FLAG', 'ENTRY_CHANNEL_NAME', 'REQUEST_TYPE_NAME', 'BUG_CNT',
       'INCIDENT_STATUS',
       'CURRENT_ENTITLEMENT_TYPE_NAME', 'INCIDENT_CONTRACT_STATUS',
       'CONTRACT NUMBER', 'CONTRACT_TYPE', 'CREATION_CONTRACT_SVC_LINE_ID',
       'HW_VERSION_ID', 'SW_VERSION_ID',
       'TAC_PRODUCT_SW_KEY', 'UPDATED_COT_TECH_KEY', 'INCIDENT_TYPE',
       'PROBLEM_CODE', 'RESOLUTION_CODE', 'PART_NUMBER', 'SOLUTION_CNT',
       'ERP_FAMILY', 'ERP_PLATFORM_NAME', 'TAC_PRODUCT_HW_KEY',
       'TAC_HW_PLATFORM_NAME', 'TECH_NAME', 'BUSINESS_UNIT_CODE',
       'HYBRID_PRODUCT_FAMILY', 'SUB_TECH_NAME', 'SR_PRODUCT_ID',
       'UNDERLYING_CAUSE_NAME', 'UNDERLYING_CAUSE_DESCRIPTION', 'CASE_NUMBER',
       'B2B_FLAG', 'TECH_ID', 'SUB_TECH_ID', 'SW_VERSION_ACT_ID',
       'SW_VERSION_NAME', 'VALID_SR_FILTER_FLAG', 'CUSTOMER_VERTICAL_CD',
       'CUSTOMER_MARKET_SEGMENT_CD', 'ISO_COUNTRY_CD',
       'Initial Time to Resolution', 'Final Time to Resolution', 'SRC_DEL_FLG',
       'Business Ownership Time', 'Customer Ownership Time',
       'Other Ownership Time', 'Delivery Ownership Time', 'CUSTOMER_NAME',
       'PARTY ID', 'PARTY NAME', 'ID', 'SERVICE_SUBGROUP_DESC',
       'SERVICE_LEVLE_CODE', 'SERVICE_PROGRAM', 'SERVICE_BRAND',
       'SR_TECHNOLOGY', 'SR_SUB_TECHNOLOGY', 'BE_INT', 'SUB_BE_INT', 'FLAG',
       'Data Extracted Date', 'RU_BK_PRODUCT_FAMILY_ID']
   
    types = tac_df.dtypes.to_dict()
    
    for t in types:
        if types[t] == "int64":
            tac_df[t]=tac_df[t].fillna(0)
        elif types[t] == "float64":
            tac_df[t]=tac_df[t].fillna(0.0)
        else:
            tac_df[t]=tac_df[t].fillna('')
            
    tac_df[['CURRENT_SEVERITY_INT','MAX_SEVERITY_INT']] = tac_df[['CURRENT_SEVERITY_INT','MAX_SEVERITY_INT']].astype('int32')
    tac_df[['SR_TIME_TO_CLOSE_DAYS_CNT']] = tac_df[['SR_TIME_TO_CLOSE_DAYS_CNT']].astype('float')
    tac_df = tac_df.drop(columns=["INCIDENT_ID"])
    tac_df["BUG_CNT"] = tac_df["BUG_CNT"].replace("",0.0)

    return tac_df



def get_appliance_data(user,ids_sav,ids_gu,ids_cr,ids_cav):
    
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

            query_appliance = f"""SELECT * FROM "CX_DB"."CX_CA_BR"."BV_CX_COLLECTOR_APPLIANCE_DETAILS"
                                WHERE CUSTOMER_ID IN ({types_list.get(type_id)}) AND CUSTOMER_IDENTIFIER = '{type_id}'"""

            cs.execute(query_appliance)
            df = cs.fetchall()

            appliance_columns = ["CR_PARTY_ID","APPLIANCE_ID","INVENTORY_NAME","COLLECTION_DATE","CUSTOMER_IDENTIFIER","CUSTOMER_ID"]

            df = pd.DataFrame(df,columns=appliance_columns)
            dfs.append(df)

    appliance_df = pd.concat(dfs)
    
    #types = uncovered_df.dtypes.to_dict()
    
    return appliance_df

def format_columns(uncovered,coverage,contracts,appliance):
    
    # CONTRACTS & COVERAGE
    coverage[['COVERED_ITEM_QTY','UNCOVERED_ITEM_QTY','UNCOVERED_SNTC_LIST_PRICE','UNCOVERED_SSPT_LIST_PRICE','PRODUCT_UNIT_LIST_PRICE']] = coverage[['COVERED_ITEM_QTY','UNCOVERED_ITEM_QTY','UNCOVERED_SNTC_LIST_PRICE','UNCOVERED_SSPT_LIST_PRICE','PRODUCT_UNIT_LIST_PRICE']].astype(float)
    new_columns_coverage = {'BUSINESS_ENTITY_DESCR':'Architecture', 
                   'RU_BK_PRODUCT_FAMILY_ID':'Product Family',
                   'BK_PRODUCT_ID':'Product ID'}
    #coverage.rename(columns=new_columns_coverage,inplace=True)
    
    # UNCOVERED DATA
    uncovered = uncovered[["ACCOUNT_ID","ACCOUNT_IDENTIFIER","ALERT_URL","APPLIANCE_ID","BILLTO_CUSTOMER_NAME","BK_PRODUCT_ID","BK_PRODUCT_TYPE_ID","BUSINESS_ENTITY_DESCR","CHANNEL_PARTNER_NAME","COLLECTION_DATE","COMPONENT_TYPE","CONTRACT_END_DATE","CONTRACT_LINE_END_DATE","CONTRACT_LINE_STATUS","CONTRACT_LINE_STATUS_FROM_IB","CONTRACT_TYPE","COUNTRY_NAME","COVERAGE","CUSTOMER_NAME","DV_GOODS_PRODUCT_CATEGORY_CD","DV_ITEM_TYPE_CD","EQUIPMENT_TYPE_DESCRIPTION","HOSTNAME","IMPORTED_BY","INSTANCE_STATUS","INVENTORY","IPADDRESS","L1_SALES_TERRITORY_DESCR","L2_SALES_TERRITORY_DESCR","LAST_SUPPORT_DT","LATEST_QUALIFICATION_STATUS","LINE_STATUS","PF_BAND","PRODUCT_CATEGORY_CD","PRODUCT_SALES_ORDER_NUMBER","REFRESH_DATE","RU_BK_PRODUCT_FAMILY_ID","SERIAL_NUMBER","SERVICE_CONTRACT_NUMBER","SERVICE_PROGRAM","SHIP_DATE","SNTC_YORN","SOURCE","SSPT_YORN","SUB_BUSINESS_ENTITY_DESCR","UPDATED_DATE","WARRANTY_TYPE","5SNTP","BASE_PRICE_USD_AMT","BK_BE_GEO_ID_INT","C2P","C4P","C4S","CONTRACT_LINE_END_FISCAL_QUARTER","CONTRACT_LINE_END_FISCAL_YEAR","CS","CUSTOMER_ID","ECMU","ECMUS","EQUIPMENT_TYPE","INSTALL_SITE_ID","INSTALLATION_QUANTITY","INSTANCE_NUMBER","LDOS_AGE","LDOS_FISCAL_YEAR","PRODUCT_UNIT_LIST_PRICE","S2P","SHIP_MONTH_AGE","SHIPPED_FISCAL_YEAR","SNC","SNT","SNTE","SNTP","SSC2P","SSC4P","SSC4S","SSCS","SSNCO","SSSNC","SSSNE","SSSNP","SSSNT"]]
    
    #new_columns_uncovered = {'PRODUCT_SALES_ORDER_NUMBER':'Product SO#',
    #                         'BK_PRODUCT_ID':'Product ID',
    #                         'PRODUCT_CATEGORY_CD':'Product Category',
    #                         'SUB_BUSINESS_ENTITY_DESCR':'Sub Architecture', 
    #                         'BUSINESS_ENTITY_DESCR':'Architecture', 
    #                         'BK_PRODUCT_TYPE_ID':'Product Type', 
    #                         'RU_BK_PRODUCT_FAMILY_ID':'Product Family',
    #                         'BILLTO_CUSTOMER_NAME':'Product Bill To',
    #                         'PF_BAND':'Product Band'}
    
    uncovered[['SERVICE_CONTRACT_NUMBER','LDOS_AGE','LDOS_FISCAL_YEAR']] = uncovered[['SERVICE_CONTRACT_NUMBER','LDOS_AGE','LDOS_FISCAL_YEAR']].fillna(0).astype("int64")
    uncovered['INSTALLATION_QUANTITY'] = uncovered['INSTALLATION_QUANTITY'].astype(float)
    uncovered['INSTANCE_NUMBER'] = uncovered['INSTANCE_NUMBER'].astype(float)
    uncovered["UPDATED_DATE"] = pd.to_datetime(uncovered["UPDATED_DATE"])
    uncovered["EQUIPMENT_TYPE"] = uncovered["EQUIPMENT_TYPE"].astype(float)
    uncovered["COLLECTION_DATE"] = pd.to_datetime(uncovered["COLLECTION_DATE"])
    uncovered["REFRESH_DATE"] = pd.to_datetime(uncovered["REFRESH_DATE"])
    uncovered[['BASE_PRICE_USD_AMT','SNT', 'C2P', 'C4P', 'C4S', 'CS', 'SNC','SNTE', 'SNTP', 'ECMU', 'S2P', 'ECMUS', 'SSC2P', 'SSC4P', 'SSC4S', 'SSCS', 'SSNCO', 'SSSNC', 'SSSNE', 'SSSNP', 'SSSNT', '5SNTP']] = uncovered[['BASE_PRICE_USD_AMT','SNT', 'C2P', 'C4P', 'C4S', 'CS', 'SNC','SNTE', 'SNTP', 'ECMU', 'S2P', 'ECMUS', 'SSC2P', 'SSC4P', 'SSC4S','SSCS', 'SSNCO', 'SSSNC', 'SSSNE', 'SSSNP', 'SSSNT', '5SNTP']].astype(float)
    uncovered["SHIP_DATE"] = pd.to_datetime(uncovered["SHIP_DATE"])
    uncovered["LAST_SUPPORT_DT"] = pd.to_datetime(uncovered["LAST_SUPPORT_DT"], errors = 'coerce')

    
    #uncovered.rename(columns=new_columns_uncovered,inplace = True)
    
    
    # CONTRACTS VIEW
    new_columns_contracts = {'CUSTOMER_ID':'Bk Sales Account Id Int',
                   'CUSTOMER_NAME':'Sales Account Group Name',
                   'BUSINESS_ENTITY_DESCR':'Business Entity Descr',
                   'BK_PRODUCT_ID':'Bk Product Id',
                   'CONTRACT_TYPE':'Contract Type',
                   'CONTRACT_LINE_END_FISCAL_QUARTER':'Contract Line End Fiscal Quarter',
                   'SNTC_SSPT_OFFER_FLAG':'Sntc Sspt Offer Flag',
                   'COVERED_ITEM_QTY':'Covered Item Qty',
                   'ANNUALIZED_EXTENDED_CONTRACT_LINE_LIST_USD_AMOUNT':'Annualized Extended Contract Line List Usd Amount',
                   "ANNUALIZED_CONTRACT_LINE_NET_USD_AMOUNT":"ANNUALIZED_CONTRACT_LINE_NET_USD_AMOUNT",
                   'PRODUCT_UNIT_LIST_PRICE':'Product Unit List Price',
                   'SERVICE_LIST_PRICE':'Service List Price',
                   "CORRECTED_CONTRACT_LINE_NET_USD_AMOUNT":"CORRECTED_CONTRACT_LINE_NET_USD_AMOUNT"}
    
    contracts.rename(columns=new_columns_contracts,inplace=True)
    contracts['Contract Line End Fiscal Quarter'] = contracts['Contract Line End Fiscal Quarter'].fillna(0).astype('int32')
    
    # APPLIANCE DETAILS
    appliance = appliance[["APPLIANCE_ID","COLLECTION_DATE","CR_PARTY_ID","CUSTOMER_IDENTIFIER","INVENTORY_NAME","CUSTOMER_ID"]]
    appliance["COLLECTION_DATE"] = pd.to_datetime(appliance["COLLECTION_DATE"])
    
    return uncovered,coverage,contracts,appliance
    
    
def convert_to_twbx(twb_name):
    shutil.make_archive(base_name = twb_name, root_dir = twb_name,format = 'zip')
    os.rename(twb_name+".zip",twb_name+".twbx")

def get_url(name):
    if name == "N/A":
        url = "N/A"
    else:
        name = name.replace(' ','')
        url = r'https://cx-tableau-stage.cisco.com/#/site/Compass/views/{}/Overview?iframeSizedToWindow=true&%3Aembed=y&%3AshowAppBanner=false&%3Adisplay_count=no&%3AshowVizHome=no&%3Atabs=no&%3Aorigin=viz_share_link&%3Atoolbar=yes'.format(name)
    return url

def upload_data_to_sf(df,user):
    
    cnn = snowflake.connector.connect(
    user=user,
    authenticator='externalbrowser',
    role='CX_CA_BUS_ANALYST_ROLE',
    warehouse='CX_CA_RPT_WH',
    database='CX_DB',
    schema='CX_CA_BR',
    account='cisco.us-east-1'
    )
    
    write_pandas(
    conn=cnn,
    df=df,
    table_name='COVERAGE_COMPASS_SN_LOG'
    #table_name='EMEA_COV_COMPASS_SN_LOG'
    )
    
warranty_types_to_exclude = ["ERAT-3YR-LTD-HW","WARR-LIFE-NBD-HW",
                     "WARR-ELTD-LIFE-HW","ERAT-LTD-LIFE-HW",
                     "WARR-LIFE-RTF-HW","WARR-LTD-LIFE-HW","WARR-ELTD-LIFE-EDU"]

def Total_Recommended_Estimate_CR(uncovered_data_filtered):
    
    if len(uncovered_data_filtered) == 0 or uncovered_data_filtered.empty:
        Recommended_Estimate=0
    else:
        uncovered_data_filtered_Y = uncovered_data_filtered[(uncovered_data_filtered["SSPT_YORN"]=="Y")&(uncovered_data_filtered["SNTC_YORN"]=="Y")]
        uncovered_data_filtered_Y = uncovered_data_filtered_Y[~uncovered_data_filtered_Y['WARRANTY_TYPE'].isin(warranty_types_to_exclude)]
        uncovered_data_filtered_Y = uncovered_data_filtered_Y[['ECMU','INSTALLATION_QUANTITY','SNT','DV_GOODS_PRODUCT_CATEGORY_CD']].fillna(0)
        uncovered_data_filtered_Y['PRICING_LIST'] = ((uncovered_data_filtered_Y['DV_GOODS_PRODUCT_CATEGORY_CD'] == 'HARDWARE')*uncovered_data_filtered_Y['SNT'] + (uncovered_data_filtered_Y['DV_GOODS_PRODUCT_CATEGORY_CD'] == 0)*uncovered_data_filtered_Y['SNT'] + (uncovered_data_filtered_Y['DV_GOODS_PRODUCT_CATEGORY_CD'] == 'SOFTWARE')*uncovered_data_filtered_Y['ECMU'])*uncovered_data_filtered_Y['INSTALLATION_QUANTITY']
        #uncovered_data_filtered_Y['PRICING_LIST'] = ((uncovered_data_filtered_Y['ECMU'] * uncovered_data_filtered_Y['INSTALLATION_QUANTITY'])) + uncovered_data_filtered_Y['SNT']
        
        Recommended_Estimate=int(round(uncovered_data_filtered_Y["PRICING_LIST"].sum(),0))
    
    return Recommended_Estimate

def Total_Recommended_Estimate_CR_SSPT(uncovered_data_filtered):
    
    if len(uncovered_data_filtered) == 0 or uncovered_data_filtered.empty:
        Recommended_Estimate_SSPT=0
    else:
        uncovered_data_filtered_Y = uncovered_data_filtered[(uncovered_data_filtered["SSPT_YORN"]=="Y")&(uncovered_data_filtered["SNTC_YORN"]=="Y")]
        uncovered_data_filtered_Y = uncovered_data_filtered_Y[~uncovered_data_filtered_Y['WARRANTY_TYPE'].isin(warranty_types_to_exclude)]
        uncovered_data_filtered_Y = uncovered_data_filtered_Y[['Ecmus','INSTALLATION_QUANTITY','Sssnt','DV_GOODS_PRODUCT_CATEGORY_CD']].fillna(0)
        uncovered_data_filtered_Y['PRICING_LIST'] = ((uncovered_data_filtered_Y['DV_GOODS_PRODUCT_CATEGORY_CD'] == 'HARDWARE')*uncovered_data_filtered_Y['Sssnt'] + (uncovered_data_filtered_Y['DV_GOODS_PRODUCT_CATEGORY_CD'] == 0)*uncovered_data_filtered_Y['Sssnt'] + (uncovered_data_filtered_Y['DV_GOODS_PRODUCT_CATEGORY_CD'] == 'SOFTWARE')*uncovered_data_filtered_Y['Ecmus'])*uncovered_data_filtered_Y['INSTALLATION_QUANTITY']
        #uncovered_data_filtered_Y['PRICING_LIST'] = ((uncovered_data_filtered_Y['ECMU'] * uncovered_data_filtered_Y['INSTALLATION_QUANTITY'])) + uncovered_data_filtered_Y['SNT']
        
        Recommended_Estimate_SSPT=int(round(uncovered_data_filtered_Y["PRICING_LIST"].sum(),0))
    
    return Recommended_Estimate_SSPT


def Total_Initial_Estimate(uncovered_data_filtered):
    
    if len(uncovered_data_filtered) == 0 or uncovered_data_filtered.empty:
        Initial_Estimate=0
    else:
        uncovered_data_filtered_IE = uncovered_data_filtered[(uncovered_data_filtered["LINE_STATUS"]=="MISS ATTACH 1mo - 3mo")|(uncovered_data_filtered["LINE_STATUS"]=="MISS ATTACH 3mo - 12mo")|(uncovered_data_filtered["LINE_STATUS"]=="EXPIRED 12mo - 24mo")]
        uncovered_data_filtered_IE = uncovered_data_filtered_IE[(uncovered_data_filtered_IE["DV_GOODS_PRODUCT_CATEGORY_CD"]=="HARDWARE")|(uncovered_data_filtered_IE["DV_GOODS_PRODUCT_CATEGORY_CD"].isna()==True)]
        uncovered_data_filtered_IE = uncovered_data_filtered_IE[(uncovered_data_filtered_IE["PF_BAND"].isna()==False)&(uncovered_data_filtered_IE["PF_BAND"]!="N/A")]
        #uncovered_data_filtered_IE = uncovered_data_filtered_IE[uncovered_data_filtered_IE["WARRANTY_TYPE"]!="Enhanced Limited Life Hardware Warranty"]       
        uncovered_data_filtered_IE = uncovered_data_filtered_IE[~uncovered_data_filtered_IE['WARRANTY_TYPE'].isin(warranty_types_to_exclude)]
        uncovered_data_filtered_IE = uncovered_data_filtered_IE[['ECMU','INSTALLATION_QUANTITY','SNT','DV_GOODS_PRODUCT_CATEGORY_CD']].fillna(0)
        uncovered_data_filtered_IE['PRICING_LIST'] = ((uncovered_data_filtered_IE['DV_GOODS_PRODUCT_CATEGORY_CD'] == 'HARDWARE')*uncovered_data_filtered_IE['SNT'] + (uncovered_data_filtered_IE['DV_GOODS_PRODUCT_CATEGORY_CD'] == 0)*uncovered_data_filtered_IE['SNT'] + (uncovered_data_filtered_IE['DV_GOODS_PRODUCT_CATEGORY_CD'] == 'SOFTWARE')*uncovered_data_filtered_IE['ECMU'])*uncovered_data_filtered_IE['INSTALLATION_QUANTITY']
        Initial_Estimate=int(round(uncovered_data_filtered_IE["PRICING_LIST"].sum(),0))
    
    return Initial_Estimate


def create_extract(name,columns,df,path):
    process_parameters = {
            # Limits the number of Hyper event log files to two.
            "log_file_max_count": "2",
            # Limits the size of Hyper event log files to 100 megabytes.
            "log_file_size_limit": "100M"
        }

    with HyperProcess(telemetry=Telemetry.SEND_USAGE_DATA_TO_TABLEAU, parameters=process_parameters) as hyper:
             # Optional connection parameters.
            # They are documented in the Tableau Hyper documentation, chapter "Connection Settings"
            # (https://help.tableau.com/current/api/hyper_api/en-us/reference/sql/connectionsettings.html).
        connection_parameters = {"lc_time": "en_US"}

        
            # Creates new Hyper file "customer.hyper".
            # Replaces file with CreateMode.CREATE_AND_REPLACE if it already exists.
        with Connection(hyper.endpoint,
                        f'{path}/Data/{name}.hyper',
                        create_mode=CreateMode.CREATE_AND_REPLACE) as connection:

            connection.catalog.create_schema('Extract')
            

            table = TableDefinition(
                # Since the table name is not prefixed with an explicit schema name, the table will reside in the default "public" namespace.
                table_name=name,
                columns=columns
            )

            connection.catalog.create_table(table)
            

            with Inserter(connection, table) as inserter:
                for index, row in df.iterrows():
                    row.replace('',0.0)
                    inserter.add_row(row)
                                    
                inserter.execute()
                
                
    hyper.shutdown()
    
    
    
def get_schema(table):
        
    tac_cols = [
        TableDefinition.Column('INCIDENT_NUMBER', SqlType.int()),
        TableDefinition.Column('CURRENT_SEVERITY_INT', SqlType.int()),
        TableDefinition.Column('MAX_SEVERITY_INT', SqlType.int()),
        TableDefinition.Column('INCDT_CREATION_DATE', SqlType.date()),
        TableDefinition.Column('INCDT_CRET_FISCAL_WEEK_NUM_INT', SqlType.int()),
        TableDefinition.Column('SR Creation FY Week', SqlType.text()),
        TableDefinition.Column('SR Creation FY Month', SqlType.text()),
        TableDefinition.Column('SR Creation FY Quarter', SqlType.text()),
        TableDefinition.Column('SR Creation Fiscal Year', SqlType.int()),
        TableDefinition.Column('CLOSED_DATE', SqlType.date()),
        TableDefinition.Column('INCDT_CLOSED_FISCAL_MONTH_NM', SqlType.text()),
        TableDefinition.Column('INCDT_CLOSED_FISCAL_QTR_NM', SqlType.text()),
        TableDefinition.Column('INCDT_CLOSED_FISCAL_YEAR', SqlType.int()),
        TableDefinition.Column('SR_TIME_TO_CLOSE_DAYS_CNT', SqlType.double()),
        TableDefinition.Column('CURRENT_SERIAL_NUMBER', SqlType.text()),
        TableDefinition.Column('COMPLEXITY_DESCR', SqlType.text()),
        TableDefinition.Column('INITIAL_PROBLEM_CODE', SqlType.text()),
        TableDefinition.Column('OUTAGE_FLAG', SqlType.text()),
        TableDefinition.Column('ENTRY_CHANNEL_NAME', SqlType.text()),
        TableDefinition.Column('REQUEST_TYPE_NAME', SqlType.text()),
        TableDefinition.Column('BUG_CNT', SqlType.double()),
        #TableDefinition.Column('INCIDENT_CONTACT_EMAIL', SqlType.text()),
        TableDefinition.Column('INCIDENT_STATUS', SqlType.text()),
        TableDefinition.Column('CURRENT_ENTITLEMENT_TYPE_NAME', SqlType.text()),
        TableDefinition.Column('INCIDENT_CONTRACT_STATUS', SqlType.text()),
        TableDefinition.Column('CONTRACT NUMBER', SqlType.text()),
        TableDefinition.Column('CONTRACT_TYPE', SqlType.text()),
        TableDefinition.Column('CREATION_CONTRACT_SVC_LINE_ID', SqlType.double()),
        #TableDefinition.Column('CURRENT_OWNER_CCO_ID', SqlType.text()),
        TableDefinition.Column('HW_VERSION_ID', SqlType.big_int()),
        TableDefinition.Column('SW_VERSION_ID', SqlType.big_int()),
        TableDefinition.Column('TAC_PRODUCT_SW_KEY', SqlType.int()),
        TableDefinition.Column('UPDATED_COT_TECH_KEY', SqlType.int()),
        TableDefinition.Column('INCIDENT_TYPE', SqlType.text()),
        TableDefinition.Column('PROBLEM_CODE', SqlType.text()),
        TableDefinition.Column('RESOLUTION_CODE', SqlType.text()),
        TableDefinition.Column('PART_NUMBER', SqlType.text()),
        TableDefinition.Column('SOLUTION_CNT', SqlType.text()),
        TableDefinition.Column('ERP_FAMILY', SqlType.text()),
        TableDefinition.Column('ERP_PLATFORM_NAME', SqlType.text()),
        TableDefinition.Column('TAC_PRODUCT_HW_KEY', SqlType.int()),
        TableDefinition.Column('TAC_HW_PLATFORM_NAME', SqlType.text()),
        TableDefinition.Column('TECH_NAME', SqlType.text()),
        TableDefinition.Column('BUSINESS_UNIT_CODE', SqlType.text()),
        TableDefinition.Column('HYBRID_PRODUCT_FAMILY', SqlType.text()),
        TableDefinition.Column('SUB_TECH_NAME', SqlType.text()),
        TableDefinition.Column('SR_PRODUCT_ID', SqlType.text()),
        TableDefinition.Column('UNDERLYING_CAUSE_NAME', SqlType.text()),
        TableDefinition.Column('UNDERLYING_CAUSE_DESCRIPTION', SqlType.text()),
        TableDefinition.Column('CASE_NUMBER', SqlType.int()),
        TableDefinition.Column('B2B_FLAG', SqlType.text()),
        TableDefinition.Column('TECH_ID', SqlType.int()),
        TableDefinition.Column('SUB_TECH_ID', SqlType.int()),
        TableDefinition.Column('SW_VERSION_ACT_ID', SqlType.double()),
        TableDefinition.Column('SW_VERSION_NAME', SqlType.text()),
        TableDefinition.Column('VALID_SR_FILTER_FLAG', SqlType.text()),
        TableDefinition.Column('CUSTOMER_VERTICAL_CD', SqlType.text()),
        TableDefinition.Column('CUSTOMER_MARKET_SEGMENT_CD', SqlType.text()),
        TableDefinition.Column('ISO_COUNTRY_CD', SqlType.text()),
        TableDefinition.Column('Initial Time to Resolution', SqlType.double()),
        TableDefinition.Column('Final Time to Resolution', SqlType.double()),
        TableDefinition.Column('SRC_DEL_FLG', SqlType.text()),
        TableDefinition.Column('Business Ownership Time', SqlType.double()),
        TableDefinition.Column('Customer Ownership Time', SqlType.double()),
        TableDefinition.Column('Other Ownership Time', SqlType.double()),
        TableDefinition.Column('Delivery Ownership Time', SqlType.double()),
        TableDefinition.Column('CUSTOMER_NAME', SqlType.text()),
        TableDefinition.Column('PARTY ID', SqlType.int()),
        TableDefinition.Column('PARTY NAME', SqlType.text()),
        TableDefinition.Column('ID', SqlType.int()),
        TableDefinition.Column('SERVICE_SUBGROUP_DESC', SqlType.text()),
        TableDefinition.Column('SERVICE_LEVLE_CODE', SqlType.text()),
        TableDefinition.Column('SERVICE_PROGRAM', SqlType.text()),
        TableDefinition.Column('SERVICE_BRAND', SqlType.text()),
        TableDefinition.Column('SR_TECHNOLOGY', SqlType.text()),
        TableDefinition.Column('SR_SUB_TECHNOLOGY', SqlType.text()),
        TableDefinition.Column('BE_INT', SqlType.text()),
        TableDefinition.Column('SUB_BE_INT', SqlType.text()),
        TableDefinition.Column('FLAG', SqlType.text()),
        TableDefinition.Column('Data Extracted Date', SqlType.date()),
        TableDefinition.Column('RU_BK_PRODUCT_FAMILY_ID', SqlType.text())
            ]
    return tac_cols