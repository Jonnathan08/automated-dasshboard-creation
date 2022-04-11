import warnings

import snowflake.connector
import pandas as pd
import numpy as np
import itertools
from datetime import datetime

from pathlib import Path

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
    oa_df = df.query("`Assigned DA` == r'{}' and Status == 'Validated' and `OP Status` == 'In Process'".format(da),
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

def import_ib_files(df):
    renewals_s = pd.DataFrame()
    coverage_s = pd.DataFrame()
    dna_s = pd.DataFrame()
    subs_s = pd.DataFrame()
    
    renewals_g = pd.DataFrame()
    coverage_g = pd.DataFrame()
    dna_g = pd.DataFrame()
    subs_g = pd.DataFrame()
    
    if 'SAV ID' in df["ID TYPE"].unique():
        renewals_s = pd.read_csv(r"..\CRBO\SAV_ID\SAV_-_OP_-_Renewals_Oppty___41594543.csv", dtype={'SAV ID':str,'Instance Shipped Fiscal Year':str})
        coverage_s = pd.read_csv(r"..\CRBO\SAV_ID\SAV_-_OP_IB_Total_Asset_View_-_Coverage___41594531.csv", dtype={'SAV ID':str})
        dna_s = pd.read_csv(r"..\CRBO\SAV_ID\SAV_-_OP_IB_Total_Asset_View_-_DNA_Appliance___41594557.csv", dtype={'SAV ID':str,'Instance Shipped Fiscal Year':str})
        subs_s = pd.read_csv(r"..\CRBO\SAV_ID\SAV_SaaS_-_Offers_Package___41594580.csv", dtype={'SAV ID':str})
        print('SAV files loaded!')
    
    if 'GU ID' in df["ID TYPE"].unique():
        renewals_g = pd.read_csv(r".\CRBO\GU_ID\GU_-_OP_-_Renewals_Oppty___41634341.csv", dtype={'Best Site GU Party ID':str,'Instance Shipped Fiscal Year':str})
        coverage_g = pd.read_csv(r".\CRBO\GU_ID\GU_-_OP_IB_Total_Asset_View_-_Coverage___41634333.csv", dtype={'Best Site GU Party ID':str})
        dna_g = pd.read_csv(r".\CRBO\GU_ID\GU_-_OP_IB_Total_Asset_View_-_DNA_Appliance___41634312.csv", dtype={'Best Site GU Party ID':str,'Instance Shipped Fiscal Year':str})
        subs_g = pd.read_csv(r".\CRBO\GU_ID\GU_-_OP_SW_Subscription___41634352.csv", dtype={'GU Party ID':str})
        print('GU files loaded!')
        
def print_ids_list(fields_df):
    sav_l = fields_df.query("`ID TYPE` == 'SAV ID'")["sav_list"].tolist() ### Inverted quotation marks to columns with spaces
    flat_sav = list(itertools.chain(*sav_l))
    print('SAVs = ' + ';'.join(flat_sav))

    gu_l = fields_df.query("`ID TYPE` == 'GU ID'")["gu_list"].tolist()
    flat_gu = list(itertools.chain(*gu_l))
    print('GUs = ' + ';'.join(flat_gu))

    cr_l = fields_df.query("`CR Party ID` != ''")["cr_list"].tolist()
    flat_cr = list(itertools.chain(*cr_l))
    print('CRs = ' + ','.join(flat_cr))

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
    
    #cr_l = fields_df.query("`CR Party ID` != ''")["cr_list"].tolist()
    #flat_cr = list(itertools.chain(*cr_l))
    #cr_str_list = separator.join(flat_cr)
    
    contracts_l = fields_df.query("`Contract ID` != ''")["contract_list"].tolist()
    flat_cr = list(itertools.chain(*cr_l))
    cr_str_list = separator.join(flat_cr)
        
    return sav_str_list,gu_str_list,cav_str_list,cr_str_list

def fill_nas(df):
    types = df.dtypes.to_dict()

    for t in types:
        if types[t] == "int64":
            df[t]=df[t].fillna(0)
        elif types[t] == "float64":
            df[t]=df[t].fillna(0.0)
        else:
            df[t]=df[t].where(df[t].notnull(), None)
    return df

def get_url(name):
    name = name.replace(' ','')
    url = r'https://cx-tableau-stage.cisco.com/#/site/Compass/views/{}/EstimatorRecommendationsSummary?iframeSizedToWindow=true&%3Aembed=y&%3AshowAppBanner=false&%3Adisplay_count=no&%3AshowVizHome=no&%3Atabs=no&%3Aorigin=viz_share_link&%3Atoolbar=yes'.format(name)
    return url

# --------------------------------------------------- Extracts

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
                        f'{path}/Extracts/{name}.hyper',
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
                    inserter.add_row(row)
                inserter.execute()
                
                
    hyper.shutdown()

def get_telemetry_df2(user,savs,gus,parties,cavs):
    
    """Get telemetry data from Snowflake by given Party ids and
    creates a DataFrame
    
    param: user - cisco e-mail address
    param: party_ids - list of given party ids"""
          
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
    types_list = {'SAV':savs,'GU':gus,'CR':parties,'CAV':cavs}
    
    for type_id in types_list.keys():
        
        ids = types_list.get(type_id)

        if ids == '': pass
        else:

            query_telemetry = f"""select distinct
CAST (I1.CUSTOMERID as string) as "Party ID",
I1.PARTYNAME as "Customer",
I1.EQUIPMENTTYPEDESC as "Equipment Type Description",
I1.APPLIANCEID as "Appliance ID",
I1.INVENTORYNAME as "Inventory",
to_date(I1.COLLECTIONDATE) as "Collection Date",
I1.INVENTORYSOURCE as "Imported By",
I1.PRODUCTID as "Product ID",
I1.PRODUCTFAMILY as "Product Family",
BSE.BK_BUSINESS_ENTITY_NAME AS "Business Entity",
BSE.BK_SUB_BUSINESS_ENTITY_NAME AS "Sub Business Entity",
BSE.BUSINESS_ENTITY_DESCR AS "Business Entity Description",
I1.SNASPRODUCTFAMILY as "PF",
I1.PRODUCTDESCRIPTION as "Product Description",
I1.EQUIPMENTTYPE as "Equipment Type",
I1.PRODUCTTYPE AS "Product Type",
I1.SERIALNUMBER as "Serial Number",
A.HW_EOL as "Last Date of Support",
A.CISCO_COM_INFO AS "Alert URL",
C.CONTRACT_NUM as "Contract Number",
C.CONTRACT_STATUS as "Contract Status",
C.COVERAGE_LINE_STATUS_TEXT as "Contract Lines Status",
C.SERVICE_PROGRAM AS "Service Program",
C.CONTRACT_END_DATE as "Contract End Date",
C.COVERAGE_LINE_END_DATE as "Contract Line End Date",
CUST.ACCOUNT_ID,
CUST.ID,
CURRENT_DATE() AS "Updated Date"

FROM"CX_DB"."CX_CXCLOUDD_BR"."IBES_INV"I1
INNER JOIN
  (SELECT* FROM(
  SELECT CUSTOMERID, SERIALNUMBER, COLLECTIONDATE
  FROM "CX_DB"."CX_CXCLOUDD_BR"."IBES_INV"
  where
  CUSTOMERID IN (SELECT PARTY_ID FROM "CX_DB"."CX_CA_BR"."ACCOUNT_ID_LOOKUP"  WHERE ACCOUNT_ID='{type_id}' AND ID IN ({ids}))
               
    AND 
       TO_DATE(COLLECTIONDATE) > ADD_MONTHS(CURRENT_DATE(),-6)
    and VALIDATED_NOTVALID = 'Recognized'
    group by CUSTOMERID, SERIALNUMBER, COLLECTIONDATE  
  QUALIFY ROW_NUMBER() OVER (PARTITION BY CUSTOMERID, SERIALNUMBER ORDER BY COLLECTIONDATE DESC) = 1
   )) I2  ON I1.CUSTOMERID = I2.CUSTOMERID AND I1.SERIALNUMBER = I2.SERIALNUMBER AND I1.COLLECTIONDATE = I2.COLLECTIONDATE

--customer ID
LEFT JOIN
( SELECT PARTY_ID,ACCOUNT_ID,ID FROM "CX_DB"."CX_CA_BR"."ACCOUNT_ID_LOOKUP"
WHERE ACCOUNT_ID='{type_id}' AND ID IN ({ids})
GROUP BY 1,2,3) CUST
ON I1.CUSTOMERID = CUST.PARTY_ID

LEFT JOIN
  (SELECT* FROM(
SELECT CUSTOMERID, SERIALNUMBER,CONTRACT_NUM, CONTRACT_STATUS, CONTRACT_END_DATE, COVERAGE_LINE_END_DATE, COVERAGE_LINE_STATUS_TEXT, CREATEDATE, SERVICE_PROGRAM, coverage_line_id,
CASE WHEN COVERAGE_LINE_STATUS_TEXT = 'ACTIVE' THEN 1
      WHEN COVERAGE_LINE_STATUS_TEXT = 'SIGNED' THEN 2
      WHEN COVERAGE_LINE_STATUS_TEXT = 'OVERDUE' THEN 3
      WHEN COVERAGE_LINE_STATUS_TEXT = 'EXPIRED' THEN 4
      WHEN COVERAGE_LINE_STATUS_TEXT = 'INACTIVE_DEL' THEN 5
      WHEN COVERAGE_LINE_STATUS_TEXT = 'QA_HOLD' THEN 6
      WHEN COVERAGE_LINE_STATUS_TEXT = NULL THEN 7
      ELSE 8 END AS COVERAGE_LINE_STATUS_INT   
    FROM "CX_DB"."CX_CXCLOUDD_BR"."IBES_CONTR"
  WHERE
  CUSTOMERID IN (SELECT PARTY_ID FROM "CX_DB"."CX_CA_BR"."ACCOUNT_ID_LOOKUP"  WHERE ACCOUNT_ID='{type_id}' AND ID IN ({ids}))
    GROUP BY
    CUSTOMERID, SERIALNUMBER,CONTRACT_NUM, CONTRACT_STATUS, CONTRACT_END_DATE, COVERAGE_LINE_END_DATE, COVERAGE_LINE_STATUS_TEXT, CREATEDATE, SERVICE_PROGRAM,coverage_line_id,COVERAGE_LINE_STATUS_INT
  QUALIFY ROW_NUMBER() OVER (PARTITION BY CUSTOMERID, SERIALNUMBER ORDER BY COVERAGE_LINE_STATUS_INT ASC) = 1 
  )
  ) C ON I1.CUSTOMERID = C.CUSTOMERID AND I1.SERIALNUMBER = C.SERIALNUMBER
//attriutes from Alert table
LEFT JOIN
(SELECT * FROM (
SELECT CUSTOMERID,SERIALNUMBER,COLLECTIONID,HW_EOL, CISCO_COM_INFO
  FROM CX_DB.CX_CXCLOUDD_BR.IBES_ALERT
  where
 CUSTOMERID IN (SELECT PARTY_ID FROM "CX_DB"."CX_CA_BR"."ACCOUNT_ID_LOOKUP"  WHERE ACCOUNT_ID='{type_id}' AND ID IN ({ids}))
   AND TYPE = 'Hardware-EOX'
    group by CUSTOMERID,SERIALNUMBER,COLLECTIONID, HW_EOL,CISCO_COM_INFO
QUALIFY ROW_NUMBER() OVER (PARTITION BY CUSTOMERID, COLLECTIONID, SERIALNUMBER,CISCO_COM_INFO ORDER BY HW_EOL DESC) = 1)
  )
A ON I1.SERIALNUMBER = A.SERIALNUMBER AND  I1.COLLECTIONID = A.COLLECTIONID 
 
left join 
  (SELECT
   BK_PRODUCT_ID,BK_BUSINESS_ENTITY_NAME,BK_SUB_BUSINESS_ENTITY_NAME,BUSINESS_ENTITY_DESCR
    FROM
CX_DB.CX_CA_EBV.BV_BE_HIER_PRDT_FAMILY_ALLOC
where
DV_FISCAL_QUARTER_ID = '2022Q2'
and BK_BUSINESS_ENTITY_TYPE_CD = 'I'
     QUALIFY ROW_NUMBER() OVER (PARTITION BY BK_PRODUCT_ID ORDER BY PRDT_FAMILY_ALLOCATION_PCT DESC) = 1) BSE ON BSE.BK_PRODUCT_ID = I1.PRODUCTID

    where I1.CUSTOMERID IN  (SELECT PARTY_ID FROM "CX_DB"."CX_CA_BR"."ACCOUNT_ID_LOOKUP"  WHERE ACCOUNT_ID='{type_id}' AND ID IN ({ids}))


            """

            cs.execute(query_telemetry)
            df = cs.fetchall()

            cir_columns = ['Party ID','Customer', 'Equipment Type Description', 'Appliance ID',
                           'Inventory', 'Collection Date', 'Imported By', 'Product ID',
                           'Product Family', 'Business Entity', 'Sub Business Entity',
                           'Business Entity Description', 'PF', 'Product Description',
                           'Equipment Type', 'Product Type', 'Serial Number',
                           'Last Date of Support', 'Alert URL', 'Contract Number',
                           'Contract Status', 'Contract Lines Status', 'Service Program',
                           'Contract End Date', 'Contract Line End Date','ACCOUNT_ID','ID','Updated Date']

            df = pd.DataFrame(df,columns=cir_columns)
            
            dfs.append(df)

    cir_df = pd.concat(dfs)
    
    #types = uncovered_df.dtypes.to_dict()
    cir_df.insert(loc=1, column='ACTIVE_YORN', value='Y')
    #cir_df = cir_df.query("ACTIVE_YORN == 'Y'")
    cir_df['Party ID'] = cir_df['Party ID'].apply(lambda x: int(0 if x is None else x))
    cir_df[['Contract End Date',"Updated Date", 'Contract Line End Date','Last Date of Support']] = cir_df[['Contract End Date',"Updated Date", 'Contract Line End Date','Last Date of Support']].replace({pd.NaT: None})
    cir_df['Equipment Type'] = cir_df['Equipment Type'].apply(lambda x: float(0.0 if x is None else str(x).split('.')[0]))
    
    return cir_df

def get_tac_df_new(user,ids,id_type): 
    
    """Get TAC data from Snowflake by given Party ids and
    creates a DataFrame
    
    param: user - cisco e-mail address
    param: sav_ids - list of given sav ids"""
    
    if ids == '':
        ids = 0
        
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
    
    if id_type == 'SAV ID':
        query_tac = f"""SELECT * FROM "CX_DB"."CX_CA_BR"."TAC_UNION"
        WHERE FLAG = 'SAV'
        AND ID in ({ids})"""
    elif id_type == 'GU ID':
        query_tac = f"""SELECT * FROM "CX_DB"."CX_CA_BR"."TAC_UNION"
        WHERE FLAG = 'GU'
        AND ID in ({ids})"""
    elif id_type == 'PARTY ID':
        query_tac = f"""SELECT * FROM "CX_DB"."CX_CA_BR"."TAC_UNION"
        WHERE FLAG = 'GU'
        AND "PARTY ID" in ({ids})"""
    elif id_type == 'CAV ID':
        query_tac = f"""SELECT * FROM "CX_DB"."CX_CA_BR"."TAC_UNION"
        WHERE FLAG = 'CAV'
        AND ID in ({ids})"""
 
    cs.execute(query_tac)
    df = cs.fetchall()
    cs.close()
    cnn.close()
    
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
       'Data Extracted Date']

    tac_df = pd.DataFrame(df,columns=tac_columns)
    
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

    return tac_df

def get_cav_names(user,ids): 
    
    """Get CAV Names from CAV IDs
    
    param: user - cisco e-mail address
    param: cav_ids - list of given cav ids"""
    
    if ids == '':
        ids = 0
        
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
    query_cav_names = f"""SELECT "CAV_ID", "CAV_NAME" FROM "CX_DB"."CX_CA_BR"."ACCOUNT_ID_LOOKUP"
        WHERE "CAV_ID" IN ({ids})
        GROUP BY "CAV_ID","CAV_NAME"
        """

    cs.execute(query_cav_names)
    df = cs.fetchall()
    cs.close()
    cnn.close()
    
    cav_columns = ['CAV ID','CAV NAME']

    cav_df = pd.DataFrame(df,columns=cav_columns)
    
    types = cav_df.dtypes.to_dict()
    
    for t in types:
        if types[t] == "int64":
            cav_df[t]=cav_df[t].fillna(0)
        elif types[t] == "float64":
            cav_df[t]=cav_df[t].fillna(0.0)
        else:
            cav_df[t]=cav_df[t].fillna('')

    return cav_df

def get_schema(table,id_type=None):
    
    ib_cols = [
        TableDefinition.Column('ID', SqlType.int()),
        TableDefinition.Column('Name', SqlType.text()),
        TableDefinition.Column('Best Site ID', SqlType.double()),
        TableDefinition.Column('Best Site Customer Name', SqlType.text()),
        TableDefinition.Column('Best Site Sales Level 2 Name', SqlType.text()),
        TableDefinition.Column('Coverage', SqlType.text()),
        TableDefinition.Column('Contract Number', SqlType.big_int()),
        TableDefinition.Column('Covered Line Status', SqlType.text()),
        TableDefinition.Column('Contract Type', SqlType.text()),
        TableDefinition.Column('Contract Line End Quarter', SqlType.int()),
        TableDefinition.Column('Contract Line End Fiscal Year', SqlType.int()),
        TableDefinition.Column('Instance Shipped Fiscal Year', SqlType.int()),
        TableDefinition.Column('Service Brand Code', SqlType.text()),
        TableDefinition.Column('Offer Type Name', SqlType.text()),
        TableDefinition.Column('Asset Type', SqlType.text()),
        TableDefinition.Column('LDoS', SqlType.date()),
        TableDefinition.Column('LDoS Fiscal Quarter', SqlType.text()),
        TableDefinition.Column('LDoS FY', SqlType.int()),
        TableDefinition.Column('Business Entity Name', SqlType.text()),
        TableDefinition.Column('Sub Business Entity Name', SqlType.text()),
        TableDefinition.Column('Product Family', SqlType.text()),
        TableDefinition.Column('Product ID', SqlType.text()),
        TableDefinition.Column('Product Type', SqlType.text()),
        TableDefinition.Column('SWSS Flag', SqlType.text()),
        TableDefinition.Column('Default Service List Price USD', SqlType.double()),
        TableDefinition.Column('Item Quantity', SqlType.double()),
        TableDefinition.Column('Annual Extended Contract Line List USD Amount', SqlType.double()),
        TableDefinition.Column('Annual Contract Line Net USD Amount', SqlType.double()),
        TableDefinition.Column('Annualized Extended Contract Line List USD Amount', SqlType.double()),
        TableDefinition.Column('Annualized Contract Line Net USD Amount', SqlType.double()),
        TableDefinition.Column('Contract Line List Price USD', SqlType.double()),
        TableDefinition.Column('Contract Line Net Price USD', SqlType.double()),
        TableDefinition.Column('Asset List Amount', SqlType.double())
        ]
    
    coverage_cols = [
        TableDefinition.Column('ID', SqlType.text(), NOT_NULLABLE),
        TableDefinition.Column('Name', SqlType.text(), NOT_NULLABLE),
        TableDefinition.Column('Coverage', SqlType.text(), NOT_NULLABLE),
        TableDefinition.Column('Item Quantity', SqlType.int(), NOT_NULLABLE),
        TableDefinition.Column('Asset List Amount', SqlType.double(), NOT_NULLABLE),
        TableDefinition.Column('Asset Net Amount', SqlType.double(), NOT_NULLABLE),
        TableDefinition.Column('Annual Contract Line Net USD Amount', SqlType.double(), NOT_NULLABLE),
        TableDefinition.Column('Contract Line Net Price USD', SqlType.double(), NOT_NULLABLE),
        TableDefinition.Column('Annualized Extended Contract Line List USD Amount', SqlType.double(), NOT_NULLABLE)
        ]
    
    sw_cols = [
        TableDefinition.Column('ID', SqlType.text(), NOT_NULLABLE),
        TableDefinition.Column('Name', SqlType.text(), NOT_NULLABLE),
        TableDefinition.Column('Sales Level 1 Name', SqlType.text(), NOT_NULLABLE),
        TableDefinition.Column('Sales Level 2 Name', SqlType.text(), NOT_NULLABLE),
        TableDefinition.Column('Subscription Reference ID', SqlType.text()),
        TableDefinition.Column('Subscription Type TAV', SqlType.text()),
        TableDefinition.Column('Asset Type', SqlType.text()),
        TableDefinition.Column('Product Family', SqlType.text()),
        TableDefinition.Column('Product ID', SqlType.text()),
        TableDefinition.Column('Product Description', SqlType.text()),
        TableDefinition.Column('Business Entity', SqlType.text()),
        TableDefinition.Column('Business Sub Entity', SqlType.text()),
        TableDefinition.Column('Contract Term End Quarter', SqlType.int()),
        TableDefinition.Column('Contract Term End Quarter Name', SqlType.text()),
        TableDefinition.Column('Asset List', SqlType.double()),
        TableDefinition.Column('Asset Net', SqlType.double()),
        TableDefinition.Column('Buying Program', SqlType.text())
        ]
    
    cir_cols = [
        TableDefinition.Column('Party ID', SqlType.int()),
        TableDefinition.Column('ACTIVE_YORN', SqlType.text(), NOT_NULLABLE),
        TableDefinition.Column('Customer', SqlType.text()),
        TableDefinition.Column('Equipment Type Description', SqlType.text()),
        TableDefinition.Column('Appliance ID', SqlType.text()),
        TableDefinition.Column('Inventory', SqlType.text()),
        TableDefinition.Column('Collection Date', SqlType.date()),
        TableDefinition.Column('Imported By', SqlType.text()),
        TableDefinition.Column('Product ID', SqlType.text()),
        TableDefinition.Column('Product Family', SqlType.text()),
        TableDefinition.Column('Business Entity', SqlType.text()),
        TableDefinition.Column('Sub Business Entity', SqlType.text()),
        TableDefinition.Column('Business Entity Description', SqlType.text()),
        TableDefinition.Column('PF', SqlType.text()),
        TableDefinition.Column('Product Description', SqlType.text()),
        TableDefinition.Column('Equipment Type', SqlType.double()),
        TableDefinition.Column('Product Type', SqlType.text()),
        TableDefinition.Column('Serial Number', SqlType.text()),
        TableDefinition.Column('Last Date of Support', SqlType.date()),
        TableDefinition.Column('Alert URL', SqlType.text()),
        TableDefinition.Column('Contract Number', SqlType.text()),
        TableDefinition.Column('Contract Status', SqlType.text()),
        TableDefinition.Column('Contract Lines Status', SqlType.text()),
        TableDefinition.Column('Service Program', SqlType.text()),
        TableDefinition.Column('Contract End Date', SqlType.date()),
        TableDefinition.Column('Contract Line End Date', SqlType.date()),
        TableDefinition.Column('Updated Date', SqlType.date())
    ]
    
    tac_cols = [
        #TableDefinition.Column('INCIDENT_ID', SqlType.double()),
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
        TableDefinition.Column('SAV_ID', SqlType.int()),
        TableDefinition.Column('CUSTOMER_NAME', SqlType.text()),
        TableDefinition.Column('PARTY ID', SqlType.int()),
        TableDefinition.Column('PARTY NAME', SqlType.text()),
        TableDefinition.Column('SAV_NAME', SqlType.text()),
        TableDefinition.Column('SERVICE_SUBGROUP_DESC', SqlType.text()),
        TableDefinition.Column('SERVICE_LEVLE_CODE', SqlType.text()),
        TableDefinition.Column('SERVICE_PROGRAM', SqlType.text()),
        TableDefinition.Column('SERVICE_BRAND', SqlType.text()),
        TableDefinition.Column('SR_TECHNOLOGY', SqlType.text()),
        TableDefinition.Column('SR_SUB_TECHNOLOGY', SqlType.text()),
        TableDefinition.Column('BE_INT', SqlType.text()),
        TableDefinition.Column('SUB_BE_INT', SqlType.text()),
        TableDefinition.Column('Data Extracted Date', SqlType.date())
    ]

    tac_cols2 = [
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
        TableDefinition.Column('Data Extracted Date', SqlType.date())
    ]
    
    smartsheet_cols = [
        TableDefinition.Column('Row Number', SqlType.int()),
        TableDefinition.Column('Request ID', SqlType.big_int()),
        TableDefinition.Column('Date Created', SqlType.date()),
        TableDefinition.Column('Assigned DA', SqlType.text()),
        TableDefinition.Column('Campaign Name', SqlType.text()),
        TableDefinition.Column('Customer Name', SqlType.text()),
        TableDefinition.Column('Input file URL', SqlType.text()),
        TableDefinition.Column('ID TYPE', SqlType.text()),
        TableDefinition.Column('SAV ID', SqlType.text()),
        TableDefinition.Column('CAV ID', SqlType.text()),
        TableDefinition.Column('CAV BU ID', SqlType.text()),
        TableDefinition.Column('GU ID', SqlType.text()),
        TableDefinition.Column('Lvl1', SqlType.text()),
        TableDefinition.Column('Lvl2 (Region)', SqlType.text()),
        TableDefinition.Column('Contract ID', SqlType.text()),
        TableDefinition.Column('Inventory Name', SqlType.text()),
        TableDefinition.Column('Appliance ID', SqlType.text()),
        TableDefinition.Column('CR Party Name', SqlType.text()),
        TableDefinition.Column('CR Party ID', SqlType.text()),
        TableDefinition.Column('Comments', SqlType.text()),
        TableDefinition.Column('DA Comments', SqlType.text()),
        TableDefinition.Column('Status', SqlType.text()),
        TableDefinition.Column('Requester Name', SqlType.text()),
        TableDefinition.Column('Parties Active Collectors', SqlType.text()), #Who should be notified on completion of Analysis
        TableDefinition.Column('OP Status', SqlType.text())        
        ]
    
    if table == 'ib':
        return ib_cols
    elif table == 'coverage':
        return coverage_cols
    elif table == 'sw':
        return sw_cols
    elif table == 'cir':
        return cir_cols
    elif table == 'tac':
        if id_type == 'GU ID':
            tac_cols.pop(70),tac_cols.pop(66)
            tac_cols.insert(69,TableDefinition.Column('GLOBAL_ULTIMATE_ID', SqlType.int()))
        else: pass
        return tac_cols
    elif table == 'tac2':
        return tac_cols2
    elif table == 'smartsheet':
        return smartsheet_cols
    
def LDoS_flag(data):
    list=[]
    for x in data:
        if x >= datetime.today():
            list.append('N')
        else:
            list.append('Y')
    return list

# Funcion calcula el valor del IB value
def SNTC_Oppty(df,sntc_mapping):
   
    try:
        sntc_mapping = sntc_mapping.rename(columns={"Product SKU":"Product SKU "})
    except:
        pass
    sntc_mapping = sntc_mapping[["Product SKU ","SNT"]]
    
    ib = df.copy()
    #ib['Item Quantity']=ib['Item Quantity'].astype(int)
    
    #Merging mapping and IB
    merge = ib.merge(sntc_mapping,right_on="Product SKU ",left_on="Product ID",how="left")

    #Transforming dates to calculate LDoS flag
    merge['LDoS']=merge['LDoS'].apply(lambda x: str(x).replace('/','-'))    
    merge['LDoS']=merge['LDoS'].apply(lambda x: str(x).replace('nan', '2100-01-01 00:00:00'))
    fechas = [datetime.strptime(str(i)[:-5],'%m-%d-%Y') if len(i)<16 else datetime.fromisoformat(str(i)) for i in merge['LDoS']]
    merge['LDoS'] = fechas
    
    #Calculating LDoS flag column
    merge['LDoS Flag']=LDoS_flag(merge['LDoS'])
    

    #Filtering data
    
    merge=merge[merge['Coverage']=='NOT COVERED']

    #merge=merge[(merge["Asset Type"]=="Hardware")]

    merge=merge[(merge['Product Type']!= 'APPLIANCE') & (merge['Product Type']!='CABLE') & (merge['Product Type']!='APPSWIND') & (merge['Product Type']!='POWER') & (merge['Product Type']!='DOC')]

    merge=merge[(merge['LDoS Flag']=='N')]

    #merge = merge[(merge['Business Entity Name'] != 'Other')]

    # Calculating IB Value

    merge["IB Oppty"] = merge["Item Quantity"]*merge["SNT"]/1000
    sntc_oppty = merge.iloc[:, np.r_[0,-1]].groupby(merge.columns[0]).sum()
    
    if len(sntc_oppty)>0:
        return sntc_oppty['IB Oppty'].sum()
    else:
        return 'N/A'


    
    