from ast import NotIn
from csv import excel
from heapq import merge
from pickle import FALSE, TRUE
from re import A
from tkinter.tix import COLUMN
import warnings
from matplotlib.pyplot import axis
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
import pandas as pd
import numpy as np
import itertools
from datetime import datetime
import webbrowser

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

from pathlib import Path

from tableauhyperapi import HyperProcess, Telemetry, \
    Connection, CreateMode, \
    NOT_NULLABLE, NULLABLE, SqlType, TableDefinition, \
    Inserter, \
    escape_name, escape_string_literal, \
    HyperException
#from tenacity import retry_if_not_result

warnings.filterwarnings("ignore")

def connec_to_sf(user):
    try:
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
        return cs,cnn
    except: 
        print(bcolors.FAIL + bcolors.BOLD + 'Error connecting to Snowflake: ' + bcolors.ENDC + "Check your credentials and VPN connection\n")
        raise

def clean_region(region):
    region = region.replace('_', '')
    region = region.split('-')

    return region[0]


def get_da_requests(da, df):
    oa_df = df.query("`Assigned DA` == r'{}' and Status == 'Validated' and `OP Status` == 'In Process'".format(da),
                     engine='python')

    fields_df = oa_df.copy()
    fields_df["SAV ID"] = fields_df["SAV ID"].apply(
        lambda x: str(x).replace(" ", '').split('.')[0])
    fields_df["GU ID"] = fields_df["GU ID"].apply(
        lambda x:  str(x).replace(" ", '').split('.')[0])
    fields_df["CAV ID"] = fields_df["CAV ID"].apply(
        lambda x: str(x).replace(" ", '').split('.')[0])
    fields_df["CR Party ID"] = fields_df["CR Party ID"].apply(
        lambda x:  str(x).replace(" ", '').split('.')[0])
    fields_df["Contract ID"] = fields_df["Contract ID"].apply(
        lambda x:  str(x).replace(" ", '').split('.')[0])
    fields_df["sav_list"] = fields_df["SAV ID"].apply(lambda x: x.split(','))
    fields_df["gu_list"] = fields_df["GU ID"].apply(lambda x: x.split(','))
    fields_df["cav_list"] = fields_df["CAV ID"].apply(lambda x: x.split(','))
    fields_df["cr_list"] = fields_df["CR Party ID"].apply(
        lambda x: x.split(','))
    fields_df["contract_list"] = fields_df["Contract ID"].apply(
        lambda x: x.split(','))
    fields_df["Lvl1"] = fields_df["Lvl1"].apply(lambda x: clean_region(x))
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
        renewals_s = pd.read_csv(r"..\CRBO\SAV_ID\SAV_-_OP_-_Renewals_Oppty___41594543.csv",
                                 dtype={'SAV ID': str, 'Instance Shipped Fiscal Year': str})
        coverage_s = pd.read_csv(
            r"..\CRBO\SAV_ID\SAV_-_OP_IB_Total_Asset_View_-_Coverage___41594531.csv", dtype={'SAV ID': str})
        dna_s = pd.read_csv(r"..\CRBO\SAV_ID\SAV_-_OP_IB_Total_Asset_View_-_DNA_Appliance___41594557.csv",
                            dtype={'SAV ID': str, 'Instance Shipped Fiscal Year': str})
        subs_s = pd.read_csv(
            r"..\CRBO\SAV_ID\SAV_SaaS_-_Offers_Package___41594580.csv", dtype={'SAV ID': str})
        print('SAV files loaded!')

    if 'GU ID' in df["ID TYPE"].unique():
        renewals_g = pd.read_csv(r".\CRBO\GU_ID\GU_-_OP_-_Renewals_Oppty___41634341.csv", dtype={
                                 'Best Site GU Party ID': str, 'Instance Shipped Fiscal Year': str})
        coverage_g = pd.read_csv(
            r".\CRBO\GU_ID\GU_-_OP_IB_Total_Asset_View_-_Coverage___41634333.csv", dtype={'Best Site GU Party ID': str})
        dna_g = pd.read_csv(r".\CRBO\GU_ID\GU_-_OP_IB_Total_Asset_View_-_DNA_Appliance___41634312.csv",
                            dtype={'Best Site GU Party ID': str, 'Instance Shipped Fiscal Year': str})
        subs_g = pd.read_csv(
            r".\CRBO\GU_ID\GU_-_OP_SW_Subscription___41634352.csv", dtype={'GU Party ID': str})
        print('GU files loaded!')


def print_ids_list(fields_df):
    # Inverted quotation marks to columns with spaces
    sav_l = fields_df.query("`ID TYPE` == 'SAV ID'")["sav_list"].tolist()
    flat_sav = list(itertools.chain(*sav_l))
    print('SAVs = ' + ';'.join(flat_sav))

    gu_l = fields_df.query("`ID TYPE` == 'GU ID'")["gu_list"].tolist()
    flat_gu = list(itertools.chain(*gu_l))
    print('GUs = ' + ';'.join(flat_gu))

    cr_l = fields_df.query("`CR Party ID` != ''")["cr_list"].tolist()
    flat_cr = list(itertools.chain(*cr_l))
    print('CRs = ' + ','.join(flat_cr))


def get_ids_list(fields_df, separator=';'):

    sav_l=",".join((fields_df[fields_df["ID_TYPE"]=="SAV ID"])["SAV_ID"].tolist())
    gu_l=",".join((fields_df[fields_df["ID_TYPE"]=="GU ID"])["GU_ID"].tolist())
    cav_l=",".join((fields_df[fields_df["ID_TYPE"]=="CAV ID"])["CAV_ID"].tolist())
    cr_l=",".join((fields_df[fields_df["ID_TYPE"]=="CR Party ID"])["CR_PARTY_ID"].tolist())

    print(f"SAV IDs: {sav_l}")
    print(f"GU IDs: {gu_l}")
    print(f"CAV IDs: {cav_l}")
    print(f"CR PARTY IDs: {cr_l}\n")

    return sav_l, gu_l, cav_l, cr_l

def get_ids_list_2(fields_df, separator=';'):
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

    return sav_str_list, gu_str_list, cav_str_list, cr_str_list


def fill_nas(df):
    types = df.dtypes.to_dict()

    for t in types:
        if types[t] == "int64":
            df[t] = df[t].fillna(0)
        elif types[t] == "float64":
            df[t] = df[t].fillna(0.0)
        else:
            df[t] = df[t].where(df[t].notnull(), None)
    return df


def get_url(name):
    name = name.replace(' ', '')
    url = r'https://cx-tableau-stage.cisco.com/#/site/Compass/views/{}/EstimatorRecommendationsSummary?iframeSizedToWindow=true&%3Aembed=y&%3AshowAppBanner=false&%3Adisplay_count=no&%3AshowVizHome=no&%3Atabs=no&%3Aorigin=viz_share_link&%3Atoolbar=yes'.format(
        name)
    return url

# --------------------------------------------------- Extracts


def create_extract(name, columns, df, path):
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


def get_telemetry_df2(user, savs, gus, parties, cavs, cs, number):
    """Get telemetry data from Snowflake by given Party ids and
    creates a DataFrame

    param: user - cisco e-mail address
    param: party_ids - list of given party ids"""
    if cs.is_closed():
        cs,cnn=connec_to_sf(user)

    dfs = []
    types_list = {'SAV': savs, 'GU': gus, 'CR': parties, 'CAV': cavs}

    for type_id in types_list.keys():

        ids = types_list.get(type_id)

        if ids == '':
            continue
        else:

            query_telemetry = f"""
                                    SELECT * FROM
                                    (select distinct
                                    CAST (I1.CUSTOMERID as string) as "Party ID",
                                    I1.PARTYNAME as "Customer",
                                    I1.EQUIPMENTTYPEDESC as "Equipment Type Description",
                                    I1.APPLIANCEID as "Appliance ID",
                                    I1.INVENTORYNAME as "Inventory",
                                    to_date(I1.COLLECTIONDATE) as "Collection Date",
                                    I1.INVENTORYSOURCE as "Imported By",
                                    I1.PRODUCTID as "Product ID",
                                    SP.RU_BK_PRODUCT_FAMILY_ID as "Product Family",
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
                                    INNER JOIN CX_DB.CX_CA_BR.BV_PRODUCTS SP
                                    ON I1.PRODUCTID=SP.BK_PRODUCT_ID 
                                        where I1.CUSTOMERID IN  (SELECT PARTY_ID FROM "CX_DB"."CX_CA_BR"."ACCOUNT_ID_LOOKUP"  WHERE ACCOUNT_ID='{type_id}' AND ID IN ({ids}))) AS TELEMETRY
                                        
                                    LEFT JOIN 

                                    (select Product_sku as "Product SKU",
                                            L12HR,
                                            L12OS,
                                            L14HR,
                                            L14OS,
                                            L1DCP,
                                            L1DS,
                                            L1NB3,
                                            L1NB5,
                                            L1NBD,
                                            L1NCD,
                                            L1NCO,
                                            L1NOS,
                                            L1SWT,
                                            L22HR,
                                            L22OS,
                                            L24H3,
                                            L24H5,
                                            L24HR,
                                            L24OS,
                                            L2DCP,
                                            L2DS,
                                            L2NB3,
                                            L2NB5,
                                            L2NBD,
                                            L2NCD,
                                            L2NCO,
                                            L2NOS,
                                            L2SWT
                                    from
                                            CX_DB.CX_CA_BR.SUCCESS_TRACK_PRICING
                                            ) AS SUCCESS_TRACKS        
                                    ON TELEMETRY."Product ID" = SUCCESS_TRACKS."Product SKU"

                                    LEFT JOIN

                                    (SELECT PRODUCT_SKU as "Product SKU",
                                        "3ECMU",
                                        "3SC4P",
                                        "3SNTP",
                                        "3SSNP",
                                        "3SSNT",
                                        "5SC4P",
                                        "5SNTP",
                                        "5SSNP",
                                        "5SSNT", 
                                        "ECMUS",
                                        "SSC2P",
                                        "SSC4P",
                                        "SSC4S",
                                        "SSCS",
                                        "SSDR5",
                                        "SSDR7",
                                        "SSNCO",
                                        "SSSNC",
                                        "SSSNE",
                                        "SSSNP",
                                        "SSSNT",
                                        'Y' AS SSPT_YORN
                                        /* "ELSUS", --Missing sspt column from mapping
                                        "SSPTS", --Missing sspt column from mapping
                                        "SSS2P", --Missing sspt column from mapping
                                        "SSTCM", --Missing sspt column from mapping
                                        "SSSW" --Missing sspt column from mapping*/
                                    from  CX_DB.CX_CA_BR.SSPT_PRICING
                                    ) AS SSPT_PRICING
                                    ON TELEMETRY."Product ID" = SSPT_PRICING."Product SKU"

                                    LEFT JOIN

                                    (SELECT PRODUCT_SKU as "Product SKU",
                                            "3C4P",
                                            "3SNT",
                                            "3SNTP",
                                            "3SSNC",
                                            "5SSNC",
                                            "C2P",
                                            "C2PL",
                                            "C4P",
                                            "C4PL",
                                            "C4S",
                                            "CS",
                                            "CSAS",
                                            "OPTLD",
                                            "OSPT",
                                            "OSPTD",
                                            "OSPTL",
                                            "RFR",
                                            "S2P",
                                            "S2PL",
                                            "SNC",
                                            "SNT",
                                            "SNT90",
                                            "SNTE",
                                            "SNTP",
                                            "SNTPL",
                                            "SW"
                                    from CX_DB.CX_CA_BR.SNTC_PRICING_LIST
                                    ) AS SNTC_PRICING

                                    ON TELEMETRY."Product ID" = SNTC_PRICING."Product SKU"

                                    LEFT JOIN 
                                    (select PF as INTERNAL_BE_PRODUCT_FAMILY, 
                                            BAND as ADJUSTED_CATEGORY 
                                    from CX_DB.CX_CA_BR.PRODUCT_BANDING
                                    group by 1,2) AS BAND
                                    ON TELEMETRY."Product Family" = BAND.INTERNAL_BE_PRODUCT_FAMILY

            """

            cs.execute(query_telemetry)
            df = cs.fetchall()

            cir_columns = ['Party ID', 'Customer', 'Equipment Type Description', 'Appliance ID',
                           'Inventory', 'Collection Date', 'Imported By', 'Product ID',
                           'Product Family', 'Business Entity', 'Sub Business Entity',
                           'Business Entity Description', 'PF', 'Product Description',
                           'Equipment Type', 'Product Type', 'Serial Number',
                           'Last Date of Support', 'Alert URL', 'Contract Number',
                           'Contract Status', 'Contract Lines Status', 'Service Program',
                           'Contract End Date', 'Contract Line End Date', 'ACCOUNT_ID', 'ID', 'Updated Date',
                           "Product SKU",
                            'L12HR',
                            'L12OS',
                            'L14HR',
                            'L14OS',
                            'L1DCP',
                            'L1DS',
                            'L1NB3',
                            'L1NB5',
                            'L1NBD',
                            'L1NCD',
                            'L1NCO',
                            'L1NOS',
                            'L1SWT',
                            'L22HR',
                            'L22OS',
                            'L24H3',
                            'L24H5',
                            'L24HR',
                            'L24OS',
                            'L2DCP',
                            'L2DS',
                            'L2NB3',
                            'L2NB5',
                            'L2NBD',
                            'L2NCD',
                            'L2NCO',
                            'L2NOS',
                            'L2SWT',
                            "Product SKU",
                            "3ECMU",
                            "3SC4P",
                            "3SNTP",
                            "3SSNP",
                            "3SSNT",
                            "5SC4P",
                            "5SNTP",
                            "5SSNP",
                            "5SSNT", 
                            "ECMUS",
                            "SSC2P",
                            "SSC4P",
                            "SSC4S",
                            "SSCS",
                            "SSDR5",
                            "SSDR7",
                            "SSNCO",
                            "SSSNC",
                            "SSSNE",
                            "SSSNP",
                            "SSSNT",
                            'SSPT_YORN',
                            "Product SKU",
                            "3C4P",
                            "3SNT",
                            "3SNTP",
                            "3SSNC",
                            "5SSNC",
                            "C2P",
                            "C2PL",
                            "C4P",
                            "C4PL",
                            "C4S",
                            "CS",
                            "CSAS",
                            "OPTLD",
                            "OSPT",
                            "OSPTD",
                            "OSPTL",
                            "RFR",
                            "S2P",
                            "S2PL",
                            "SNC",
                            "SNT",
                            "SNT90",
                            "SNTE",
                            "SNTP",
                            "SNTPL",
                            "SW",
                            'INTERNAL_BE_PRODUCT_FAMILY',
                            'ADJUSTED_CATEGORY']

            df = pd.DataFrame(df, columns=cir_columns)

            dfs.append(df)

    cir_df = pd.concat(dfs)

    #types = uncovered_df.dtypes.to_dict()
    #cir_df.insert(loc=1, column='ACTIVE_YORN', value='Y')
    #cir_df = cir_df.query("ACTIVE_YORN == 'Y'")
    cir_df['Party ID'] = cir_df['Party ID'].apply(
        lambda x: int(0 if x is None else x))
    cir_df[['Contract End Date', "Updated Date", 'Contract Line End Date', 'Last Date of Support']] = cir_df[[
        'Contract End Date', "Updated Date", 'Contract Line End Date', 'Last Date of Support']].replace({pd.NaT: None})
    cir_df['Equipment Type'] = cir_df['Equipment Type'].apply(
        lambda x: float(0.0 if x is None else str(x).split('.')[0]))
    
    print(f"{number}) Telemetry data downloaded!")

    return cir_df

def get_ib_data(user, savs, gus, parties, cavs ,cs,number):
    
    """Get TAC data from Snowflake by given Party ids and
    creates a DataFrame

    param: user - cisco e-mail address
    param: sav_ids - list of given sav ids"""
    if cs.is_closed():
        cs,cnn=connec_to_sf(user)

    dfs = []
    
    types_list = {'SAV': savs, 'GU': gus, 'CR': parties, 'CAV': cavs}

    for type_id in types_list.keys():

        ids = types_list.get(type_id)

        if ids == '':
            continue
        
        else:
            query_ib = f"""SELECT CUSTOMER_ID,
                                CUSTOMER_NAME,
                                L2_SALES_TERRITORY_DESCR,
                                COVERAGE,
                                SERVICE_CONTRACT_NUMBER,
                                CONTRACT_LINE_STATUS,
                                CONTRACT_TYPE,
                                CONTRACT_LINE_END_FISCAL_QUARTER,
                                CONTRACT_LINE_END_FISCAL_YEAR,
                                SHIPPED_FISCAL_YEAR,
                                SERVICE_BRAND_CODE,
                                OFFER_TYPE_NAME,
                                ASSET_TYPE,
                                LAST_SUPPORT_DT,
                                LDOS_FISCAL_QUARTER,
                                LDOS_FISCAL_YEAR,
                                BUSINESS_ENTITY_DESCR,
                                SUB_BUSINESS_ENTITY_DESCR,
                                RU_BK_PRODUCT_FAMILY_ID,
                                BK_PRODUCT_ID,
                                BK_PRODUCT_TYPE_ID,
                                sum(SERVICE_MAPPED_PRICE),
                                sum(INSTALLATION_QUANTITY),
                                sum(ANNUAL_EXTENDED_CONTRACT_LINE_LIST_USD_AMOUNT),
                                sum(ANNUAL_CONTRACT_LINE_NET_USD_AMOUNT),
                                sum(ANNUALIZED_EXTENDED_CONTRACT_LINE_LIST_USD_AMOUNT),
                                sum(ANNUALIZED_CONTRACT_LINE_NET_USD_AMOUNT),
                                sum(CONTRACT_LINE_LIST_USD_AMOUNT),
                                sum(CONTRACT_LINE_NET_USD_AMOUNT),
                                sum(ASSET_LIST_PRICE),
                                CON_PART_NUMBER,
                                CON_DESCRIPTION,
                                CON_CONVERTSTO,
                                CON_SNTC_SSPT_OFFER_FLAG,
                                CON_CURRENT_SSPT_FLAG,
                                CON_COMMENTS,
                                SSPT_YORN,
                                HTEC_OFFER_CATEGORY_L1,
                                CS_SERVICE_LEVEL,
                                CS_SPM_EQUIVALENT,
                                CS_SNTC_OR_SSPT,
                                CS_MULTIPLIER,
                                CS_UPLIFT,
                                PF_BAND,
                                ACCOUNT_IDENTIFIER,
                                CHANNEL_PARTNER_NAME,
                                IFNULL(sum(SNT),0) as "sntc_pricing",
                                //IFNULL(sum(SSSNT),0) as "sspt_pricing",
                                (IFNULL(sum(L1NBD),0) + IFNULL(sum(L1SWT),0)) as "success_tracks_pricing_L1",
                                (IFNULL(sum(L2NBD),0) + IFNULL(sum(L2SWT),0)) as "success_tracks_pricing_L2",
                                SUCCESS_TRACK_YORN,
                                IFNULL(sum(Uplift_Recommended_SL),0) as "sspt_pricing"
                                FROM(
                                    SELECT *,
                                    CASE  
                                        WHEN CS_SERVICE_LEVEL = '24x7x2' THEN SSS2P /// AQUI DEBERIA IR COLUMNA SSS2P
                                        WHEN CS_SERVICE_LEVEL = '24x7x2OS' THEN SSC2P
                                        WHEN CS_SERVICE_LEVEL = '24x7x4' THEN SSSNP
                                        WHEN CS_SERVICE_LEVEL = '24x7x4OS' THEN SSC4P
                                        WHEN CS_SERVICE_LEVEL = '8x5x4' THEN SSSNE
                                        WHEN CS_SERVICE_LEVEL = '8x5x4OS' THEN SSC4S
                                        WHEN CS_SERVICE_LEVEL = 'NBD' THEN SSSNT
                                        WHEN CS_SERVICE_LEVEL = '8x5xNDBOS' THEN SSCS
                                        WHEN CS_SERVICE_LEVEL = 'DR 24x7x4OS' THEN SSDR7
                                        WHEN CS_SERVICE_LEVEL = 'DR 8x5xNDBOS' THEN SSDR5
                                        WHEN CS_SERVICE_LEVEL = 'SNTC NO RMA' THEN SSDR5 // AQUI DEBERIA IR SSSW
                                        ELSE 0
                                    END AS CS_Multiplier_SL,
                                    CASE
                                        WHEN CS_UPLIFT IS NULL THEN 0
                                        ELSE (CS_Multiplier_SL * CS_MULTIPLIER)
                                    END AS CS_SSPT_Multiplier,
                                    CASE
                                        WHEN CONTRACT_TYPE='C2P' THEN IFNULL(SSC2P,SSSNT)
                                        WHEN CONTRACT_TYPE='3C4P' THEN IFNULL(SSC4P,SSSNT)
                                        WHEN CONTRACT_TYPE='C4P' THEN IFNULL(SSC4P,SSSNT)
                                        WHEN CONTRACT_TYPE='C4S' THEN IFNULL(SSC4S,SSSNT)
                                        WHEN CONTRACT_TYPE='CS' THEN IFNULL(SSCS,SSSNT)
                                        WHEN CONTRACT_TYPE='UCSD5' THEN IFNULL(SSDR5,SSSNT)
                                        WHEN CONTRACT_TYPE='UCSD7' THEN IFNULL(SSDR7,SSSNT)
                                        WHEN CONTRACT_TYPE='S2P' THEN IFNULL(SSS2P,SSSNT)  /// AQUI DEBERIA IR COLUMNA SSS2P en vez de SSC2P
                                        WHEN CONTRACT_TYPE='SNTE' THEN IFNULL(SSSNE,SSSNT)
                                        WHEN CONTRACT_TYPE='3SNTP' THEN IFNULL(SSSNP,0)  
                                        WHEN CONTRACT_TYPE='SNTP' THEN IFNULL(SSSNP,SSSNT)
                                        WHEN CONTRACT_TYPE='5SNTP' THEN IFNULL(SSSNP,0)
                                        WHEN CONTRACT_TYPE='3SNT' THEN IFNULL(SSSNT,0)
                                        WHEN CONTRACT_TYPE='5SNT' THEN IFNULL(SSSNT,0)
                                        WHEN CONTRACT_TYPE='SNT' THEN IFNULL(SSSNT,0)  
                                        WHEN CONTRACT_TYPE='SW' THEN IFNULL(SSSNT,SSDR5)  // AQUI DEBERIA IR SSSW en vez SSDR5
                                        //SSSPT SW
                                        WHEN CONTRACT_TYPE='ECMU' THEN IFNULL(ECMUS,0)  
                                        WHEN CON_SNTC_SSPT_OFFER_FLAG='Combined Services' THEN CS_SSPT_Multiplier  
                                        WHEN CON_SNTC_SSPT_OFFER_FLAG='SP Base'AND CONTRACT_TYPE='SPAR1' THEN SSSNT
                                        WHEN CON_SNTC_SSPT_OFFER_FLAG='SP Base' AND CONTRACT_TYPE='SPAR2' THEN IFNULL(SSSNE,SSSNT)  
                                        WHEN CON_SNTC_SSPT_OFFER_FLAG='SP Base' AND CONTRACT_TYPE='SPAR3' THEN IFNULL(SSSNP,SSSNT)
                                        WHEN CON_SNTC_SSPT_OFFER_FLAG ='SP Base' AND CONTRACT_TYPE='SPAR4' THEN IFNULL(SSS2P,SSSNT) /// AQUI DEBERIA IR COLUMNA SSS2P en vez de SSC2P
                                        WHEN CON_SNTC_SSPT_OFFER_FLAG='SP Base' AND CONTRACT_TYPE='SPC2P' THEN IFNULL(SSC2P,SSSNT)
                                        WHEN CON_SNTC_SSPT_OFFER_FLAG='SP Base' AND CONTRACT_TYPE='SPC4P' THEN SSC4P  
                                        WHEN CON_SNTC_SSPT_OFFER_FLAG='SP Base' AND CONTRACT_TYPE ='SBAR1' THEN SSSNT
                                        WHEN CON_SNTC_SSPT_OFFER_FLAG ='SP Base'AND CONTRACT_TYPE='SPCS' THEN IFNULL(SSCS,SSSNT)  
                                        WHEN CON_SNTC_SSPT_OFFER_FLAG ='TELEPRESENCE CUSTOMERS' AND CONTRACT_TYPE='ECDN' THEN SSSNT
                                        WHEN CON_SNTC_SSPT_OFFER_FLAG='TELEPRESENCE CUSTOMERS'AND CONTRACT_TYPE='ECDO' THEN SSCS  
                                        //Next Calendar Day
                                        WHEN CONTRACT_TYPE='SNC' THEN IFNULL(SSSNC,0)
                                        WHEN CONTRACT_TYPE='SNCO' THEN IFNULL(SSNCO,SSSNT)
                                        //Partner Support
                                        WHEN CONTRACT_TYPE='PSUP' THEN IFNULL(SSSNP,SSSNT)
                                        WHEN CONTRACT_TYPE='PSRT' THEN SSSNT
                                        WHEN CONTRACT_TYPE='PSUT' THEN SSSNT
                                        //Other
                                        WHEN CONTRACT_TYPE='LSNT' THEN SSSNT
                                        ELSE 0  
                                    END AS Uplift_Recommended_SL
                            FROM ("CX_DB"."CX_CA_BR"."BV_OE_IB_ASSET_VW")
                                WHERE ACCOUNT_IDENTIFIER = '{type_id}'
                                AND
                                CUSTOMER_ID IN ({ids})
                                AND
                                SERVICE_BRAND_CODE NOT IN ('PARTNER BRANDED')
                                )
                            group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,31,32,33,34,35,36,37,38,39,40,41,42,43,44,45,46,50
                        """

            cs.execute(query_ib)
            df = cs.fetchall()
            # cs.close()
            # cnn.close()
            
            ib_df_columns = ['customer_id', 
                            'customer_name',
                            'Best Site Sales Level 2 Name', 
                            'Coverage', 
                            'Contract Number', 
                            'Covered Line Status', 
                            'Contract Type', 
                            'Contract Line End Quarter',
                            'Contract Line End Fiscal Year', 
                            'Instance Shipped Fiscal Year', 
                            'Service Brand Code', 
                            'Offer Type Name', 
                            'Asset Type',
                            'LDoS', 
                            'LDoS Fiscal Quarter', 
                            'LDoS FY', 
                            'Business Entity Name', 
                            'Sub Business Entity Name', 
                            'Product Family',
                            'Product ID', 
                            'Product Type', 
                            'Default Service List Price USD', 
                            'Item Quantity',
                            'Annual Extended Contract Line List USD Amount', 
                            'Annual Contract Line Net USD Amount',
                            'Annualized Extended Contract Line List USD Amount', 
                            'Annualized Contract Line Net USD Amount',
                            'Contract Line List Price USD', 
                            'Contract Line Net Price USD',
                            'Asset List Amount',
                            'Part Number Example',
                            'Description',
                            'Convertsto',
                            'SNTC SSPT Offer Flag',
                            'Current SSPT Flag',
                            'Comments',
                            'Eligible',
                            'Offer Category L1(CX FAST)',
                            'Service Level',
                            'SPM Equivalent',
                            'SNTC/SSPT',
                            'Multiplier',
                            'Uplift',
                            'ADJUSTED_CATEGORY',
                            'ACCOUNT_IDENTIFIER',
                            'CHANNEL_PARTNER_NAME',
                            'sntc_pricing',
                            'success_tracks_pricing_L1',
                            'success_tracks_pricing_L2',
                            'SUCCESS_TRACK_YORN',
                            'sspt_pricing']

            ib_df = pd.DataFrame(df, columns=ib_df_columns)
            
            if len(ib_df) <= 0:
                print(f'No {type_id} data')
                continue

            ib_df = ib_df.astype({'customer_id': str, 
                                'customer_name': str,
                                'Best Site Sales Level 2 Name': str,
                                'Coverage': str,
                                #'Contract Number': int, # big_int
                                'Covered Line Status': str,
                                'Contract Type': str,
                                # 'Contract Line End Quarter': int,
                                # 'Contract Line End Fiscal Year': int,
                                # 'Instance Shipped Fiscal Year': int,
                                'Service Brand Code': str,
                                'Offer Type Name': str,
                                'Asset Type': str,
                                #'LDoS':datetime,
                                'LDoS Fiscal Quarter': str,
                                #'LDoS FY': int,
                                'Business Entity Name': str,
                                'Sub Business Entity Name': str,
                                'Product Family': str,
                                'Product ID': str,
                                'Product Type': str,
                                'Default Service List Price USD': float,
                                'Item Quantity': float,
                                'Annual Extended Contract Line List USD Amount': float,
                                'Annual Contract Line Net USD Amount': float,
                                'Annualized Extended Contract Line List USD Amount': float,
                                'Annualized Contract Line Net USD Amount': float,
                                'Contract Line List Price USD': float,
                                'Contract Line Net Price USD': float,
                                'Asset List Amount': float,
                                'Multiplier':float,
                                'Uplift':str,
                                'sntc_pricing':float,
                                'success_tracks_pricing_L1':float,
                                #'success_tracks_pricing_L2':float,
                                #'SUCCESS_TRACK_YORN':float,
                                #'sspt_pricing':float
                                })
            
            dfs.append(ib_df)
            
    ib_df_final = pd.concat(dfs)
    print(f"{number}) IB data downloaded!")
    return ib_df_final

def get_coverage_data(user, savs, gus, parties, cavs, cs, number):

    """Get Coverage data from Snowflake by given Identifier type

    param: user - cisco e-mail address
    param: sav_ids - list of given sav ids"""
    if cs.is_closed():
        cs,cnn=connec_to_sf(user)
    
    dfs = []
    
    types_list = {'SAV': savs, 'GU': gus, 'CR': parties, 'CAV': cavs}

    for type_id in types_list.keys():
        ids = types_list.get(type_id)

        if ids == '':
            continue
        
        else:
            
            query_coverage = f"""SELECT CUSTOMER_ID,
                                CUSTOMER_NAME,
                                COVERAGE,
                                sum(INSTALLATION_QUANTITY),
                                SUM(ASSET_LIST_PRICE),
                                sum(PRODUCT_NET_PRICE),
                                sum(ANNUAL_CONTRACT_LINE_NET_USD_AMOUNT),
                                sum(CONTRACT_LINE_NET_USD_AMOUNT),
                                sum(ANNUALIZED_EXTENDED_CONTRACT_LINE_LIST_USD_AMOUNT)
                                FROM CX_DB.CX_CA_BR.BV_OE_IB_ASSET_TEMP
                                WHERE ACCOUNT_IDENTIFIER = '{type_id}'
                                AND 
                                CUSTOMER_ID IN ({ids})
                                AND PRODUCT_CATEGORY_CD = 'Hardware'
                                group by CUSTOMER_ID,CUSTOMER_NAME,COVERAGE
                        """

            cs.execute(query_coverage)
            df = cs.fetchall()        
                        
            coverage_columns = ['CUSTOMER_ID', 'CUSTOMER_NAME', 'Coverage', 'Item Quantity', 'Asset List Amount', 'Asset Net Amount', 'Annual Contract Line Net USD Amount',
                        'Contract Line Net Price USD', 'Annualized Extended Contract Line List USD Amount']  
            
            df = pd.DataFrame(df, columns=coverage_columns)
            
            if len(df) <= 0:
                print(f'No {type_id} data')
                continue 
            
            dfs.append(df)
            
    coverage_df = pd.concat(dfs)
    print(f"{number}) Coverage data downloaded!")
    return coverage_df

def get_dna_df(user, savs, gus, parties, cavs, cs, number):
    """Get telemetry data from Snowflake by given Party ids and
    creates a DataFrame

    param: user - cisco e-mail address
    param: party_ids - list of given party ids"""
    if cs.is_closed():
        cs,cnn=connec_to_sf(user)

    dfs = []
    types_list = {'SAV': savs, 'GU': gus, 'CR': parties, 'CAV': cavs}

    for type_id in types_list.keys():

        ids = types_list.get(type_id)
        
        if ids == '':
            continue

        else:
            if type_id == 'SAV':
                query_lifecycle = f"""                     
                                    WITH CX AS (
                                    SELECT
                                        CX_CUSTOMER_BU_ID,
                                        CX_CUSTOMER_BU_KEY,
                                        CX_CUSTOMER_BU_NAME,
                                        CX_CUSTOMER_ID,
                                        CX_CUSTOMER_KEY,
                                        CX_CUSTOMER_NAME,
                                        SALES.BK_SALES_ACCOUNT_ID_INT,
                                        SALES_ACCOUNT_GROUP_NAME
                                    FROM
                                        CX_DB.CX_CA_EBV.BV_CXCUST_BU_PARTY_MAPPING CUST
                                        INNER JOIN CX_DB.CX_CA_EBV."BV_CUSTOMER_PARTY_HIERARCHY" cust1 ON CUST1.BRANCH_PARTY_SSOT_PARTY_ID_INT = CUST.CR_PARTY_ID
                                        INNER JOIN CX_DB.CX_CA_BR.BV_SA_GRPCSTPTY_LNKSYSVW_TV SAVG ON CUST1.branch_customer_party_key = SAVG.CUSTOMER_PARTY_KEY
                                        INNER JOIN CX_DB.CX_CA_BR.BV_SLS_ACCT_GROUP_SAV_PARTY SALES ON SALES.SALES_ACCOUNT_GROUP_PARTY_KEY = SAVG.SALES_ACCOUNT_GROUP_PARTY_KEY
                                    WHERE
                                        SAVG.END_TV_DT = '3500-01-01'
                                        AND SAVG.SOURCE_DELETED_FLG = 'N'
                                        AND SALES.SALES_ACCOUNT_GROUP_TYPE_CD IN ('NAMED_ACCOUNT', 'GEO_ACCOUNT')
                                        AND BK_SALES_ACCOUNT_ID_INT in ({ids})
                                    GROUP BY
                                        CX_CUSTOMER_BU_ID,
                                        CX_CUSTOMER_BU_KEY,
                                        CX_CUSTOMER_BU_NAME,
                                        CX_CUSTOMER_ID,
                                        CX_CUSTOMER_KEY,
                                        CX_CUSTOMER_NAME,
                                        SALES.BK_SALES_ACCOUNT_ID_INT,
                                        SALES_ACCOUNT_GROUP_NAME
                                    ),
                                    CSH AS (
                                    SELECT
                                        DISTINCT LEVEL3_COMP_KEY AS USE_CASE_KEY,
                                        LEVEL3_COMP_ID AS CX_USECASE_ID,
                                        LEVEL3_UC_NAME AS CX_USECASE_NAME,
                                        BK_PRODUCT_ID --K2_PRODUCT_CATEGORY_NM AS PRODUCT
                                    FROM
                                        CX_DB.CX_CA_EBV.BV_CX_SOLUTION_HIERARCHY CX_SOL
                                    where LEVEL1_SOL_NAME='Campus Network'
                                    GROUP BY
                                        1,
                                        2,
                                        3,
                                        4
                                    ),
                                    CTE AS (
                                    SELECT
                                        CX.CX_CUSTOMER_ID,
                                        CX.CX_CUSTOMER_NAME,
                                        CX.CX_CUSTOMER_BU_ID,
                                        CX_CUSTOMER_KEY,
                                        BK_CX_LIFECYCLE_STAGE_HIER_ID,
                                        LC_STG.CX_LIFECYCLE_STAGE_HIER_KEY,
                                        CX_USECASE_NAME,
                                        LC_STG.BK_CX_LIFECYCLE_STAGE_NAME,
                                        HIER.CX_SOLUTION_HIER_USE_CASE_KEY,
                                        LC_STG.CURRENT_STAGE_FLG,
                                        CUSTOMER_ELIGIBLE_FLG,
                                        CX.BK_SALES_ACCOUNT_ID_INT,
                                        CX.SALES_ACCOUNT_GROUP_NAME
                                    FROM
                                        CX_DB.CX_CA_EBV.BV_CX_CUST_BU_LC_STG_CMPLTN LC_STG
                                        INNER JOIN (
                                        SELECT
                                            CX_LIFECYCLE_STAGE_HIER_KEY,
                                            CX_SOLUTION_HIER_USE_CASE_KEY,
                                            BK_CX_LIFECYCLE_STAGE_HIER_ID
                                        FROM
                                            CX_DB.CX_CA_EBV.BV_CX_LIFECYCLE_STAGE_HIERARCHY
                                        WHERE
                                            END_TV_DTM = '3500-01-01'
                                            AND EDWSF_SOURCE_DELETED_FLAG = 'N'
                                        GROUP BY
                                            1,
                                            2,
                                            3
                                        ) HIER ON LC_STG.CX_LIFECYCLE_STAGE_HIER_KEY = HIER.CX_LIFECYCLE_STAGE_HIER_KEY
                                        INNER JOIN CSH ON CSH.USE_CASE_KEY = HIER.CX_SOLUTION_HIER_USE_CASE_KEY
                                        INNER JOIN CX ON LC_STG.CX_CUSTOMER_BU_KEY = CX.CX_CUSTOMER_BU_KEY
                                    WHERE
                                        LC_STG.CUSTOMER_ELIGIBLE_FLG <> 'N'
                                    GROUP BY
                                        1,
                                        2,
                                        3,
                                        4,
                                        5,
                                        6,
                                        7,
                                        8,
                                        9,
                                        10,
                                        11,
                                        12,
                                        13
                                    ),
                                    SAV AS (
                                    SELECT
                                        BK_SALES_ACCOUNT_ID_INT CUSTOMER_ID,
                                        SALES_ACCOUNT_GROUP_NAME CUSTOMER_NAME,
                                        CTE.CX_USECASE_NAME,
                                        MAX_LIFECYCLE_STAGE_NAME,
                                        'SAV' AS ACCOUNT_INDENTIFIER -- , MIN(BOOKINGS_PROCESS_DATE) ACTUAL_PURCHASE_DATE
                                    FROM
                                        CTE CTE
                                        LEFT JOIN (
                                        SELECT
                                            CX_CUSTOMER_KEY,
                                            BK_CX_LIFECYCLE_STAGE_NAME AS MAX_LIFECYCLE_STAGE_NAME,
                                            CX_USECASE_NAME
                                        FROM
                                            (
                                            SELECT
                                                CX_CUSTOMER_KEY,
                                                BK_CX_LIFECYCLE_STAGE_NAME,
                                                CX_USECASE_NAME,
                                                ROW_NUMBER() OVER (
                                                PARTITION BY CX_CUSTOMER_KEY,
                                                CX_USECASE_NAME
                                                ORDER BY
                                                    CASE
                                                    BK_CX_LIFECYCLE_STAGE_NAME
                                                    WHEN 'Advocate' THEN 1
                                                    WHEN 'Optimize' THEN 2
                                                    WHEN 'Adopt' THEN 3
                                                    WHEN 'Engage' THEN 4
                                                    WHEN 'Use' THEN 5
                                                    WHEN 'Implement' THEN 6
                                                    WHEN 'Onboard' THEN 7
                                                    WHEN 'Purchase' THEN 8
                                                    END
                                                ) AS RN
                                            FROM
                                                CTE
                                            WHERE
                                                CURRENT_STAGE_FLG = 'Y'
                                                AND CUSTOMER_ELIGIBLE_FLG <> 'N'
                                            ) STAGE
                                        WHERE
                                            RN = 1
                                        ) LC_STG_AUTO ON CTE.CX_CUSTOMER_KEY = LC_STG_AUTO.CX_CUSTOMER_KEY
                                        AND CTE.CX_USECASE_NAME = LC_STG_AUTO.CX_USECASE_NAME
                                        /*left
                                        JOIN
                                        (
                                        SELECT  PRODUCT_NAME
                                        ,CX_CUSTOMER_BU_KEY
                                        ,MIN(BOOKINGS_PROCESS_DATE) BOOKINGS_PROCESS_DATE
                                        FROM CX_DB.CX_CA_EBV.BV_CX_BOOKINGS
                                        WHERE end_customer_party_id not IN ('-999')
                                        GROUP BY  1
                                        ,2
                                        ) BKG
                                        ON BKG.PRODUCT_NAME=CTE.BK_PRODUCT_ID AND BKG.CX_CUSTOMER_BU_KEY=CTE.CX_CUSTOMER_BU_KEY*/
                                    GROUP BY
                                        1,
                                        2,
                                        3,
                                        4
                                    )
                                    SELECT
                                    A.CUSTOMER_ID,
                                    A.CUSTOMER_NAME,
                                    A.CX_USECASE_NAME,
                                    C.MAX_LIFECYCLE_STAGE_NAME,
                                    A.ACCOUNT_INDENTIFIER,
                                    B.MAX_LIFECYCLE_STAGE_NAME CUSTOMER_MAX_LIFE_CYCLE_STAGE
                                    FROM
                                    SAV A
                                    LEFT JOIN (
                                        SELECT
                                        CUSTOMER_ID,
                                        MAX_LIFECYCLE_STAGE_NAME,
                                        ROW_NUMBER() OVER (
                                            PARTITION BY CUSTOMER_ID
                                            ORDER BY
                                            CASE
                                                MAX_LIFECYCLE_STAGE_NAME
                                                WHEN 'Advocate' THEN 1
                                                WHEN 'Optimize' THEN 2
                                                WHEN 'Adopt' THEN 3
                                                WHEN 'Engage' THEN 4
                                                WHEN 'Use' THEN 5
                                                WHEN 'Implement' THEN 6
                                                WHEN 'Onboard' THEN 7
                                                WHEN 'Purchase' THEN 8
                                            END
                                        ) AS RN
                                        FROM
                                        SAV
                                    ) B ON A.CUSTOMER_ID = b.CUSTOMER_ID
                                    LEFT JOIN (
                                        SELECT
                                        CUSTOMER_ID,
                                        CX_USECASE_NAME,
                                        MAX_LIFECYCLE_STAGE_NAME,
                                        ROW_NUMBER() OVER (
                                            PARTITION BY CUSTOMER_ID,
                                            CX_USECASE_NAME
                                            ORDER BY
                                            CASE
                                                MAX_LIFECYCLE_STAGE_NAME
                                                WHEN 'Advocate' THEN 1
                                                WHEN 'Optimize' THEN 2
                                                WHEN 'Adopt' THEN 3
                                                WHEN 'Engage' THEN 4
                                                WHEN 'Use' THEN 5
                                                WHEN 'Implement' THEN 6
                                                WHEN 'Onboard' THEN 7
                                                WHEN 'Purchase' THEN 8
                                            END ASC
                                        ) AS RN
                                        FROM
                                        SAV
                                    ) C ON A.CUSTOMER_ID = C.CUSTOMER_ID
                                    AND A.CX_USECASE_NAME = C.CX_USECASE_NAME
                                    WHERE
                                    1 = 1
                                    AND b.RN = 1
                                    AND c.RN = 1
                                    GROUP BY
                                    1,
                                    2,
                                    3,
                                    4,
                                    5,
                                    6;
                            """
                
            elif type_id == 'GU':
                query_lifecycle = f"""                     
                                    WITH CX AS (
                                                        SELECT
                                                            CX_CUSTOMER_BU_ID,
                                                            CX_CUSTOMER_BU_KEY,
                                                            CX_CUSTOMER_BU_NAME,
                                                            CX_CUSTOMER_ID,
                                                            CX_CUSTOMER_KEY,
                                                            CX_CUSTOMER_NAME,
                                                            cust1.GU_PARTY_SSOT_PARTY_ID_INT,
                                                            cust1.GU_PRIMARY_NAME
                                                        FROM
                                                            CX_DB.CX_CA_EBV.BV_CXCUST_BU_PARTY_MAPPING CUST
                                                            INNER JOIN CX_DB.CX_CA_EBV."BV_CUSTOMER_PARTY_HIERARCHY" cust1 ON CUST1.BRANCH_PARTY_SSOT_PARTY_ID_INT = CUST.CR_PARTY_ID
                                                    WHERE CUST1.GU_PARTY_SSOT_PARTY_ID_INT in ({ids})
                                                        GROUP BY
                                                            CX_CUSTOMER_BU_ID,
                                                            CX_CUSTOMER_BU_KEY,
                                                            CX_CUSTOMER_BU_NAME,
                                                            CX_CUSTOMER_ID,
                                                            CX_CUSTOMER_KEY,
                                                            CX_CUSTOMER_NAME,
                                                            cust1.GU_PARTY_SSOT_PARTY_ID_INT,
                                                            cust1.GU_PRIMARY_NAME
                                                        ),
                                                        CSH AS (
                                                        SELECT
                                                            DISTINCT LEVEL3_COMP_KEY AS USE_CASE_KEY,
                                                            LEVEL3_COMP_ID AS CX_USECASE_ID,
                                                            LEVEL3_UC_NAME AS CX_USECASE_NAME,
                                                            BK_PRODUCT_ID --K2_PRODUCT_CATEGORY_NM AS PRODUCT
                                                        FROM
                                                            CX_DB.CX_CA_EBV.BV_CX_SOLUTION_HIERARCHY CX_SOL
                                                            where LEVEL1_SOL_NAME='Campus Network'
                                                        GROUP BY
                                                            1,
                                                            2,
                                                            3,
                                                            4
                                                        ),
                                                        CTE AS (
                                                        SELECT
                                                            CX.CX_CUSTOMER_ID,
                                                            CX.CX_CUSTOMER_NAME,
                                                            CX.CX_CUSTOMER_BU_ID,
                                                            CX_CUSTOMER_KEY,
                                                            BK_CX_LIFECYCLE_STAGE_HIER_ID,
                                                            LC_STG.CX_LIFECYCLE_STAGE_HIER_KEY,
                                                            CX_USECASE_NAME,
                                                            LC_STG.BK_CX_LIFECYCLE_STAGE_NAME,
                                                            HIER.CX_SOLUTION_HIER_USE_CASE_KEY,
                                                            LC_STG.CURRENT_STAGE_FLG,
                                                            CUSTOMER_ELIGIBLE_FLG,
                                                            CX.GU_PARTY_SSOT_PARTY_ID_INT,
                                                            CX.GU_PRIMARY_NAME
                                                        FROM
                                                            CX_DB.CX_CA_EBV.BV_CX_CUST_BU_LC_STG_CMPLTN LC_STG
                                                            INNER JOIN (
                                                            SELECT
                                                                CX_LIFECYCLE_STAGE_HIER_KEY,
                                                                CX_SOLUTION_HIER_USE_CASE_KEY,
                                                                BK_CX_LIFECYCLE_STAGE_HIER_ID
                                                            FROM
                                                                CX_DB.CX_CA_EBV.BV_CX_LIFECYCLE_STAGE_HIERARCHY
                                                            WHERE
                                                                END_TV_DTM = '3500-01-01'
                                                                AND EDWSF_SOURCE_DELETED_FLAG = 'N'
                                                            GROUP BY
                                                                1,
                                                                2,
                                                                3
                                                            ) HIER ON LC_STG.CX_LIFECYCLE_STAGE_HIER_KEY = HIER.CX_LIFECYCLE_STAGE_HIER_KEY
                                                            INNER JOIN CSH ON CSH.USE_CASE_KEY = HIER.CX_SOLUTION_HIER_USE_CASE_KEY
                                                            INNER JOIN CX ON LC_STG.CX_CUSTOMER_BU_KEY = CX.CX_CUSTOMER_BU_KEY
                                                        WHERE
                                                            LC_STG.CUSTOMER_ELIGIBLE_FLG <> 'N'
                                                        GROUP BY
                                                            1,
                                                            2,
                                                            3,
                                                            4,
                                                            5,
                                                            6,
                                                            7,
                                                            8,
                                                            9,
                                                            10,
                                                            11,
                                                            12,
                                                            13
                                                        ),
                                                        SAV AS (
                                                        SELECT
                                                            GU_PARTY_SSOT_PARTY_ID_INT CUSTOMER_ID,
                                                            GU_PRIMARY_NAME CUSTOMER_NAME,
                                                            CTE.CX_USECASE_NAME,
                                                            MAX_LIFECYCLE_STAGE_NAME,
                                                            'GU' AS ACCOUNT_INDENTIFIER -- , MIN(BOOKINGS_PROCESS_DATE) ACTUAL_PURCHASE_DATE
                                                        FROM
                                                            CTE CTE
                                                            LEFT JOIN (
                                                            SELECT
                                                                CX_CUSTOMER_KEY,
                                                                BK_CX_LIFECYCLE_STAGE_NAME AS MAX_LIFECYCLE_STAGE_NAME,
                                                                CX_USECASE_NAME
                                                            FROM
                                                                (
                                                                SELECT
                                                                    CX_CUSTOMER_KEY,
                                                                    BK_CX_LIFECYCLE_STAGE_NAME,
                                                                    CX_USECASE_NAME,
                                                                    ROW_NUMBER() OVER (
                                                                    PARTITION BY CX_CUSTOMER_KEY,
                                                                    CX_USECASE_NAME
                                                                    ORDER BY
                                                                        CASE
                                                                        BK_CX_LIFECYCLE_STAGE_NAME
                                                                        WHEN 'Advocate' THEN 1
                                                                        WHEN 'Optimize' THEN 2
                                                                        WHEN 'Adopt' THEN 3
                                                                        WHEN 'Engage' THEN 4
                                                                        WHEN 'Use' THEN 5
                                                                        WHEN 'Implement' THEN 6
                                                                        WHEN 'Onboard' THEN 7
                                                                        WHEN 'Purchase' THEN 8
                                                                        END
                                                                    ) AS RN
                                                                FROM
                                                                    CTE
                                                                WHERE
                                                                    CURRENT_STAGE_FLG = 'Y'
                                                                    AND CUSTOMER_ELIGIBLE_FLG <> 'N'
                                                                ) STAGE
                                                            WHERE
                                                                RN = 1
                                                            ) LC_STG_AUTO ON CTE.CX_CUSTOMER_KEY = LC_STG_AUTO.CX_CUSTOMER_KEY
                                                            AND CTE.CX_USECASE_NAME = LC_STG_AUTO.CX_USECASE_NAME
                                                            /*left
                                                            JOIN
                                                            (
                                                            SELECT  PRODUCT_NAME
                                                            ,CX_CUSTOMER_BU_KEY
                                                            ,MIN(BOOKINGS_PROCESS_DATE) BOOKINGS_PROCESS_DATE
                                                            FROM CX_DB.CX_CA_EBV.BV_CX_BOOKINGS
                                                            WHERE end_customer_party_id not IN ('-999')
                                                            GROUP BY  1
                                                            ,2
                                                            ) BKG
                                                            ON BKG.PRODUCT_NAME=CTE.BK_PRODUCT_ID AND BKG.CX_CUSTOMER_BU_KEY=CTE.CX_CUSTOMER_BU_KEY*/
                                                        GROUP BY
                                                            1,
                                                            2,
                                                            3,
                                                            4
                                                        )
                                                        SELECT
                                                        A.CUSTOMER_ID,
                                                        A.CUSTOMER_NAME,
                                                        A.CX_USECASE_NAME,
                                                        C.MAX_LIFECYCLE_STAGE_NAME,
                                                        A.ACCOUNT_INDENTIFIER,
                                                        B.MAX_LIFECYCLE_STAGE_NAME CUSTOMER_MAX_LIFE_CYCLE_STAGE
                                                        FROM
                                                        SAV A
                                                        LEFT JOIN (
                                                            SELECT
                                                            CUSTOMER_ID,
                                                            MAX_LIFECYCLE_STAGE_NAME,
                                                            ROW_NUMBER() OVER (
                                                                PARTITION BY CUSTOMER_ID
                                                                ORDER BY
                                                                CASE
                                                                    MAX_LIFECYCLE_STAGE_NAME
                                                                    WHEN 'Advocate' THEN 1
                                                                    WHEN 'Optimize' THEN 2
                                                                    WHEN 'Adopt' THEN 3
                                                                    WHEN 'Engage' THEN 4
                                                                    WHEN 'Use' THEN 5
                                                                    WHEN 'Implement' THEN 6
                                                                    WHEN 'Onboard' THEN 7
                                                                    WHEN 'Purchase' THEN 8
                                                                END
                                                            ) AS RN
                                                            FROM
                                                            SAV
                                                        ) B ON A.CUSTOMER_ID = b.CUSTOMER_ID
                                                        LEFT JOIN (
                                                            SELECT
                                                            CUSTOMER_ID,
                                                            CX_USECASE_NAME,
                                                            MAX_LIFECYCLE_STAGE_NAME,
                                                            ROW_NUMBER() OVER (
                                                                PARTITION BY CUSTOMER_ID,
                                                                CX_USECASE_NAME
                                                                ORDER BY
                                                                CASE
                                                                    MAX_LIFECYCLE_STAGE_NAME
                                                                    WHEN 'Advocate' THEN 1
                                                                    WHEN 'Optimize' THEN 2
                                                                    WHEN 'Adopt' THEN 3
                                                                    WHEN 'Engage' THEN 4
                                                                    WHEN 'Use' THEN 5
                                                                    WHEN 'Implement' THEN 6
                                                                    WHEN 'Onboard' THEN 7
                                                                    WHEN 'Purchase' THEN 8
                                                                END ASC
                                                            ) AS RN
                                                            FROM
                                                            SAV
                                                        ) C ON A.CUSTOMER_ID = C.CUSTOMER_ID
                                                        AND A.CX_USECASE_NAME = C.CX_USECASE_NAME
                                                        WHERE
                                                        1 = 1
                                                        AND b.RN = 1
                                                        AND c.RN = 1
                                                        GROUP BY
                                                        1,
                                                        2,
                                                        3,
                                                        4,
                                                        5,
                                                        6;

                            """
                
            elif type_id == 'CAV':
                query_lifecycle = f"""                     
                                    WITH CX AS (
                                                        SELECT
                                                            CX_CUSTOMER_BU_ID,
                                                            CX_CUSTOMER_BU_KEY,
                                                            CX_CUSTOMER_BU_NAME,
                                                            CX_CUSTOMER_ID,
                                                            CX_CUSTOMER_KEY,
                                                            CX_CUSTOMER_NAME
                                                        
                                                        FROM
                                                            CX_DB.CX_CA_EBV.BV_CXCUST_BU_PARTY_MAPPING CUST
                                                        WHERE CX_CUSTOMER_ID in ({ids})
                                                        GROUP BY
                                                            CX_CUSTOMER_BU_ID,
                                                            CX_CUSTOMER_BU_KEY,
                                                            CX_CUSTOMER_BU_NAME,
                                                            CX_CUSTOMER_ID,
                                                            CX_CUSTOMER_KEY,
                                                            CX_CUSTOMER_NAME
                                                            
                                                        ),
                                                        CSH AS (
                                                        SELECT
                                                            DISTINCT LEVEL3_COMP_KEY AS USE_CASE_KEY,
                                                            LEVEL3_COMP_ID AS CX_USECASE_ID,
                                                            LEVEL3_UC_NAME AS CX_USECASE_NAME,
                                                            BK_PRODUCT_ID --K2_PRODUCT_CATEGORY_NM AS PRODUCT
                                                        FROM
                                                            CX_DB.CX_CA_EBV.BV_CX_SOLUTION_HIERARCHY CX_SOL
                                                        where LEVEL1_SOL_NAME='Campus Network'
                                                        GROUP BY
                                                            1,
                                                            2,
                                                            3,
                                                            4
                                                        ),
                                                        CTE AS (
                                                        SELECT
                                                            CX.CX_CUSTOMER_ID,
                                                            CX.CX_CUSTOMER_NAME,
                                                            CX.CX_CUSTOMER_BU_ID,
                                                            CX_CUSTOMER_KEY,
                                                            BK_CX_LIFECYCLE_STAGE_HIER_ID,
                                                            LC_STG.CX_LIFECYCLE_STAGE_HIER_KEY,
                                                            CX_USECASE_NAME,
                                                            LC_STG.BK_CX_LIFECYCLE_STAGE_NAME,
                                                            HIER.CX_SOLUTION_HIER_USE_CASE_KEY,
                                                            LC_STG.CURRENT_STAGE_FLG,
                                                            CUSTOMER_ELIGIBLE_FLG
                                                        FROM
                                                            CX_DB.CX_CA_EBV.BV_CX_CUST_BU_LC_STG_CMPLTN LC_STG
                                                            INNER JOIN (
                                                            SELECT
                                                                CX_LIFECYCLE_STAGE_HIER_KEY,
                                                                CX_SOLUTION_HIER_USE_CASE_KEY,
                                                                BK_CX_LIFECYCLE_STAGE_HIER_ID
                                                            FROM
                                                                CX_DB.CX_CA_EBV.BV_CX_LIFECYCLE_STAGE_HIERARCHY
                                                            WHERE
                                                                END_TV_DTM = '3500-01-01'
                                                                AND EDWSF_SOURCE_DELETED_FLAG = 'N'
                                                            GROUP BY
                                                                1,
                                                                2,
                                                                3
                                                            ) HIER ON LC_STG.CX_LIFECYCLE_STAGE_HIER_KEY = HIER.CX_LIFECYCLE_STAGE_HIER_KEY
                                                            INNER JOIN CSH ON CSH.USE_CASE_KEY = HIER.CX_SOLUTION_HIER_USE_CASE_KEY
                                                            INNER JOIN CX ON LC_STG.CX_CUSTOMER_BU_KEY = CX.CX_CUSTOMER_BU_KEY
                                                        WHERE
                                                            LC_STG.CUSTOMER_ELIGIBLE_FLG <> 'N'
                                                        GROUP BY
                                                            1,
                                                            2,
                                                            3,
                                                            4,
                                                            5,
                                                            6,
                                                            7,
                                                            8,
                                                            9,
                                                            10,
                                                            11
                                                            
                                                        ),
                                                        SAV AS (
                                                        SELECT
                                                            CX_CUSTOMER_ID CUSTOMER_ID,
                                                            CX_CUSTOMER_NAME CUSTOMER_NAME,
                                                            CTE.CX_USECASE_NAME,
                                                            MAX_LIFECYCLE_STAGE_NAME,
                                                            'CAV' AS ACCOUNT_INDENTIFIER -- , MIN(BOOKINGS_PROCESS_DATE) ACTUAL_PURCHASE_DATE
                                                        FROM
                                                            CTE CTE
                                                            LEFT JOIN (
                                                            SELECT
                                                                CX_CUSTOMER_KEY,
                                                                BK_CX_LIFECYCLE_STAGE_NAME AS MAX_LIFECYCLE_STAGE_NAME,
                                                                CX_USECASE_NAME
                                                            FROM
                                                                (
                                                                SELECT
                                                                    CX_CUSTOMER_KEY,
                                                                    BK_CX_LIFECYCLE_STAGE_NAME,
                                                                    CX_USECASE_NAME,
                                                                    ROW_NUMBER() OVER (
                                                                    PARTITION BY CX_CUSTOMER_KEY,
                                                                    CX_USECASE_NAME
                                                                    ORDER BY
                                                                        CASE
                                                                        BK_CX_LIFECYCLE_STAGE_NAME
                                                                        WHEN 'Advocate' THEN 1
                                                                        WHEN 'Optimize' THEN 2
                                                                        WHEN 'Adopt' THEN 3
                                                                        WHEN 'Engage' THEN 4
                                                                        WHEN 'Use' THEN 5
                                                                        WHEN 'Implement' THEN 6
                                                                        WHEN 'Onboard' THEN 7
                                                                        WHEN 'Purchase' THEN 8
                                                                        END
                                                                    ) AS RN
                                                                FROM
                                                                    CTE
                                                                WHERE
                                                                    CURRENT_STAGE_FLG = 'Y'
                                                                    AND CUSTOMER_ELIGIBLE_FLG <> 'N'
                                                                ) STAGE
                                                            WHERE
                                                                RN = 1
                                                            ) LC_STG_AUTO ON CTE.CX_CUSTOMER_KEY = LC_STG_AUTO.CX_CUSTOMER_KEY
                                                            AND CTE.CX_USECASE_NAME = LC_STG_AUTO.CX_USECASE_NAME
                                                            /*left
                                                            JOIN
                                                            (
                                                            SELECT  PRODUCT_NAME
                                                            ,CX_CUSTOMER_BU_KEY
                                                            ,MIN(BOOKINGS_PROCESS_DATE) BOOKINGS_PROCESS_DATE
                                                            FROM CX_DB.CX_CA_EBV.BV_CX_BOOKINGS
                                                            WHERE end_customer_party_id not IN ('-999')
                                                            GROUP BY  1
                                                            ,2
                                                            ) BKG
                                                            ON BKG.PRODUCT_NAME=CTE.BK_PRODUCT_ID AND BKG.CX_CUSTOMER_BU_KEY=CTE.CX_CUSTOMER_BU_KEY*/
                                                        GROUP BY
                                                            1,
                                                            2,
                                                            3,
                                                            4
                                                        )
                                                        SELECT
                                                        A.CUSTOMER_ID,
                                                        A.CUSTOMER_NAME,
                                                        A.CX_USECASE_NAME,
                                                        C.MAX_LIFECYCLE_STAGE_NAME,
                                                        A.ACCOUNT_INDENTIFIER,
                                                        B.MAX_LIFECYCLE_STAGE_NAME CUSTOMER_MAX_LIFE_CYCLE_STAGE
                                                        FROM
                                                        SAV A
                                                        LEFT JOIN (
                                                            SELECT
                                                            CUSTOMER_ID,
                                                            MAX_LIFECYCLE_STAGE_NAME,
                                                            ROW_NUMBER() OVER (
                                                                PARTITION BY CUSTOMER_ID
                                                                ORDER BY
                                                                CASE
                                                                    MAX_LIFECYCLE_STAGE_NAME
                                                                    WHEN 'Advocate' THEN 1
                                                                    WHEN 'Optimize' THEN 2
                                                                    WHEN 'Adopt' THEN 3
                                                                    WHEN 'Engage' THEN 4
                                                                    WHEN 'Use' THEN 5
                                                                    WHEN 'Implement' THEN 6
                                                                    WHEN 'Onboard' THEN 7
                                                                    WHEN 'Purchase' THEN 8
                                                                END
                                                            ) AS RN
                                                            FROM
                                                            SAV
                                                        ) B ON A.CUSTOMER_ID = b.CUSTOMER_ID
                                                        LEFT JOIN (
                                                            SELECT
                                                            CUSTOMER_ID,
                                                            CX_USECASE_NAME,
                                                            MAX_LIFECYCLE_STAGE_NAME,
                                                            ROW_NUMBER() OVER (
                                                                PARTITION BY CUSTOMER_ID,
                                                                CX_USECASE_NAME
                                                                ORDER BY
                                                                CASE
                                                                    MAX_LIFECYCLE_STAGE_NAME
                                                                    WHEN 'Advocate' THEN 1
                                                                    WHEN 'Optimize' THEN 2
                                                                    WHEN 'Adopt' THEN 3
                                                                    WHEN 'Engage' THEN 4
                                                                    WHEN 'Use' THEN 5
                                                                    WHEN 'Implement' THEN 6
                                                                    WHEN 'Onboard' THEN 7
                                                                    WHEN 'Purchase' THEN 8
                                                                END ASC
                                                            ) AS RN
                                                            FROM
                                                            SAV
                                                        ) C ON A.CUSTOMER_ID = C.CUSTOMER_ID
                                                        AND A.CX_USECASE_NAME = C.CX_USECASE_NAME
                                                        WHERE
                                                        1 = 1
                                                        AND b.RN = 1
                                                        AND c.RN = 1
                                                        GROUP BY
                                                        1,
                                                        2,
                                                        3,
                                                        4,
                                                        5,
                                                        6;

                            """
                
            elif type_id == 'CR':
                query_lifecycle = f"""                     
                                    WITH CX AS (
                                                        SELECT
                                                            CX_CUSTOMER_BU_ID,
                                                            CX_CUSTOMER_BU_KEY,
                                                            CX_CUSTOMER_BU_NAME,
                                                            CX_CUSTOMER_ID,
                                                            CX_CUSTOMER_KEY,
                                                            CX_CUSTOMER_NAME,
                                                            cust1.BRANCH_PARTY_SSOT_PARTY_ID_INT,
                                                            cust1.BRANCH_PRIMARY_NAME
                                                        FROM
                                                            CX_DB.CX_CA_EBV.BV_CXCUST_BU_PARTY_MAPPING CUST
                                                            INNER JOIN CX_DB.CX_CA_EBV."BV_CUSTOMER_PARTY_HIERARCHY" cust1 ON CUST1.BRANCH_PARTY_SSOT_PARTY_ID_INT = CUST.CR_PARTY_ID
                                                        WHERE CUST1.BRANCH_PARTY_SSOT_PARTY_ID_INT in ({ids})
                                                        GROUP BY
                                                            CX_CUSTOMER_BU_ID,
                                                            CX_CUSTOMER_BU_KEY,
                                                            CX_CUSTOMER_BU_NAME,
                                                            CX_CUSTOMER_ID,
                                                            CX_CUSTOMER_KEY,
                                                            CX_CUSTOMER_NAME,
                                                            cust1.BRANCH_PARTY_SSOT_PARTY_ID_INT,
                                                            cust1.BRANCH_PRIMARY_NAME
                                                        ),
                                                        CSH AS (
                                                        SELECT
                                                            DISTINCT LEVEL3_COMP_KEY AS USE_CASE_KEY,
                                                            LEVEL3_COMP_ID AS CX_USECASE_ID,
                                                            LEVEL3_UC_NAME AS CX_USECASE_NAME,
                                                            BK_PRODUCT_ID --K2_PRODUCT_CATEGORY_NM AS PRODUCT
                                                        FROM
                                                            CX_DB.CX_CA_EBV.BV_CX_SOLUTION_HIERARCHY CX_SOL
                                                        where LEVEL1_SOL_NAME='Campus Network'
                                                        GROUP BY
                                                            1,
                                                            2,
                                                            3,
                                                            4
                                                        ),
                                                        CTE AS (
                                                        SELECT
                                                            CX.CX_CUSTOMER_ID,
                                                            CX.CX_CUSTOMER_NAME,
                                                            CX.CX_CUSTOMER_BU_ID,
                                                            CX_CUSTOMER_KEY,
                                                            BK_CX_LIFECYCLE_STAGE_HIER_ID,
                                                            LC_STG.CX_LIFECYCLE_STAGE_HIER_KEY,
                                                            CX_USECASE_NAME,
                                                            LC_STG.BK_CX_LIFECYCLE_STAGE_NAME,
                                                            HIER.CX_SOLUTION_HIER_USE_CASE_KEY,
                                                            LC_STG.CURRENT_STAGE_FLG,
                                                            CUSTOMER_ELIGIBLE_FLG,
                                                            CX.BRANCH_PARTY_SSOT_PARTY_ID_INT,
                                                            CX.BRANCH_PRIMARY_NAME
                                                        FROM
                                                            CX_DB.CX_CA_EBV.BV_CX_CUST_BU_LC_STG_CMPLTN LC_STG
                                                            INNER JOIN (
                                                            SELECT
                                                                CX_LIFECYCLE_STAGE_HIER_KEY,
                                                                CX_SOLUTION_HIER_USE_CASE_KEY,
                                                                BK_CX_LIFECYCLE_STAGE_HIER_ID
                                                            FROM
                                                                CX_DB.CX_CA_EBV.BV_CX_LIFECYCLE_STAGE_HIERARCHY
                                                            WHERE
                                                                END_TV_DTM = '3500-01-01'
                                                                AND EDWSF_SOURCE_DELETED_FLAG = 'N'
                                                            GROUP BY
                                                                1,
                                                                2,
                                                                3
                                                            ) HIER ON LC_STG.CX_LIFECYCLE_STAGE_HIER_KEY = HIER.CX_LIFECYCLE_STAGE_HIER_KEY
                                                            INNER JOIN CSH ON CSH.USE_CASE_KEY = HIER.CX_SOLUTION_HIER_USE_CASE_KEY
                                                            INNER JOIN CX ON LC_STG.CX_CUSTOMER_BU_KEY = CX.CX_CUSTOMER_BU_KEY
                                                        WHERE
                                                            LC_STG.CUSTOMER_ELIGIBLE_FLG <> 'N'
                                                        GROUP BY
                                                            1,
                                                            2,
                                                            3,
                                                            4,
                                                            5,
                                                            6,
                                                            7,
                                                            8,
                                                            9,
                                                            10,
                                                            11,
                                                            12,
                                                            13
                                                        ),
                                                        SAV AS (
                                                        SELECT
                                                            BRANCH_PARTY_SSOT_PARTY_ID_INT CUSTOMER_ID,
                                                            BRANCH_PRIMARY_NAME CUSTOMER_NAME,
                                                            CTE.CX_USECASE_NAME,
                                                            MAX_LIFECYCLE_STAGE_NAME,
                                                            'CR' AS ACCOUNT_INDENTIFIER -- , MIN(BOOKINGS_PROCESS_DATE) ACTUAL_PURCHASE_DATE
                                                        FROM
                                                            CTE CTE
                                                            LEFT JOIN (
                                                            SELECT
                                                                CX_CUSTOMER_KEY,
                                                                BK_CX_LIFECYCLE_STAGE_NAME AS MAX_LIFECYCLE_STAGE_NAME,
                                                                CX_USECASE_NAME
                                                            FROM
                                                                (
                                                                SELECT
                                                                    CX_CUSTOMER_KEY,
                                                                    BK_CX_LIFECYCLE_STAGE_NAME,
                                                                    CX_USECASE_NAME,
                                                                    ROW_NUMBER() OVER (
                                                                    PARTITION BY CX_CUSTOMER_KEY,
                                                                    CX_USECASE_NAME
                                                                    ORDER BY
                                                                        CASE
                                                                        BK_CX_LIFECYCLE_STAGE_NAME
                                                                        WHEN 'Advocate' THEN 1
                                                                        WHEN 'Optimize' THEN 2
                                                                        WHEN 'Adopt' THEN 3
                                                                        WHEN 'Engage' THEN 4
                                                                        WHEN 'Use' THEN 5
                                                                        WHEN 'Implement' THEN 6
                                                                        WHEN 'Onboard' THEN 7
                                                                        WHEN 'Purchase' THEN 8
                                                                        END
                                                                    ) AS RN
                                                                FROM
                                                                    CTE
                                                                WHERE
                                                                    CURRENT_STAGE_FLG = 'Y'
                                                                    AND CUSTOMER_ELIGIBLE_FLG <> 'N'
                                                                ) STAGE
                                                            WHERE
                                                                RN = 1
                                                            ) LC_STG_AUTO ON CTE.CX_CUSTOMER_KEY = LC_STG_AUTO.CX_CUSTOMER_KEY
                                                            AND CTE.CX_USECASE_NAME = LC_STG_AUTO.CX_USECASE_NAME
                                                            /*left
                                                            JOIN
                                                            (
                                                            SELECT  PRODUCT_NAME
                                                            ,CX_CUSTOMER_BU_KEY
                                                            ,MIN(BOOKINGS_PROCESS_DATE) BOOKINGS_PROCESS_DATE
                                                            FROM CX_DB.CX_CA_EBV.BV_CX_BOOKINGS
                                                            WHERE end_customer_party_id not IN ('-999')
                                                            GROUP BY  1
                                                            ,2
                                                            ) BKG
                                                            ON BKG.PRODUCT_NAME=CTE.BK_PRODUCT_ID AND BKG.CX_CUSTOMER_BU_KEY=CTE.CX_CUSTOMER_BU_KEY*/
                                                        GROUP BY
                                                            1,
                                                            2,
                                                            3,
                                                            4
                                                        )
                                                        SELECT
                                                        A.CUSTOMER_ID,
                                                        A.CUSTOMER_NAME,
                                                        A.CX_USECASE_NAME,
                                                        C.MAX_LIFECYCLE_STAGE_NAME,
                                                        A.ACCOUNT_INDENTIFIER,
                                                        B.MAX_LIFECYCLE_STAGE_NAME CUSTOMER_MAX_LIFE_CYCLE_STAGE
                                                        FROM
                                                        SAV A
                                                        LEFT JOIN (
                                                            SELECT
                                                            CUSTOMER_ID,
                                                            MAX_LIFECYCLE_STAGE_NAME,
                                                            ROW_NUMBER() OVER (
                                                                PARTITION BY CUSTOMER_ID
                                                                ORDER BY
                                                                CASE
                                                                    MAX_LIFECYCLE_STAGE_NAME
                                                                    WHEN 'Advocate' THEN 1
                                                                    WHEN 'Optimize' THEN 2
                                                                    WHEN 'Adopt' THEN 3
                                                                    WHEN 'Engage' THEN 4
                                                                    WHEN 'Use' THEN 5
                                                                    WHEN 'Implement' THEN 6
                                                                    WHEN 'Onboard' THEN 7
                                                                    WHEN 'Purchase' THEN 8
                                                                END
                                                            ) AS RN
                                                            FROM
                                                            SAV
                                                        ) B ON A.CUSTOMER_ID = b.CUSTOMER_ID
                                                        LEFT JOIN (
                                                            SELECT
                                                            CUSTOMER_ID,
                                                            CX_USECASE_NAME,
                                                            MAX_LIFECYCLE_STAGE_NAME,
                                                            ROW_NUMBER() OVER (
                                                                PARTITION BY CUSTOMER_ID,
                                                                CX_USECASE_NAME
                                                                ORDER BY
                                                                CASE
                                                                    MAX_LIFECYCLE_STAGE_NAME
                                                                    WHEN 'Advocate' THEN 1
                                                                    WHEN 'Optimize' THEN 2
                                                                    WHEN 'Adopt' THEN 3
                                                                    WHEN 'Engage' THEN 4
                                                                    WHEN 'Use' THEN 5
                                                                    WHEN 'Implement' THEN 6
                                                                    WHEN 'Onboard' THEN 7
                                                                    WHEN 'Purchase' THEN 8
                                                                END ASC
                                                            ) AS RN
                                                            FROM
                                                            SAV
                                                        ) C ON A.CUSTOMER_ID = C.CUSTOMER_ID
                                                        AND A.CX_USECASE_NAME = C.CX_USECASE_NAME
                                                        WHERE
                                                        1 = 1
                                                        AND b.RN = 1
                                                        AND c.RN = 1
                                                        GROUP BY
                                                        1,
                                                        2,
                                                        3,
                                                        4,
                                                        5,
                                                        6;

                            """
                
                
            cs.execute(query_lifecycle)
            df = cs.fetchall()
            
            dna_columns = ['CUSTOMER_ID',	'CX_USECASE_NAME',
                           'MAX_LIFECYCLE_STAGE_NAME', 'CUSTOMER_MAX_LIFE_CYCLE_STAGE', 'CUSTOMER_IDENTIFIER', 'STAGE']
            
            df = pd.DataFrame(df, columns= dna_columns)
            
            if len(df) <= 0:
                print(f'No {type_id} data')
                continue
            
            dfs.append(df)
            

    dna_df = pd.concat(dfs)
    print(f"{number}) DNA data downloaded!")
    return dna_df

def get_tac_df_new(user, savs, gus, parties, cavs, cs, number):
    """Get TAC data from Snowflake by given Party ids and
    creates a DataFrame

    param: user - cisco e-mail address
    param: sav_ids - list of given sav ids"""
    if cs.is_closed():
        cs,cnn=connec_to_sf(user)

    dfs = []
    
    types_list = {'SAV': savs, 'GU': gus, 'CR': parties, 'CAV': cavs}

    #print("Downloading TAC data...")
    for id_type in types_list.keys():

        ids = types_list.get(id_type)

        if ids == '':
            continue
        
        else:
            
            query_tac = f"""SELECT * FROM "CX_DB"."CX_CA_BR"."BV_OE_TAC_UNION_VW"
            WHERE FLAG = '{id_type}'
            AND ID in ({ids})
            AND TO_DATE(INCDT_CREATION_DATE) > ADD_MONTHS(CURRENT_DATE(),-24)
            """
            
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
                        'Data Extracted Date','RU_BK_PRODUCT_FAMILY_ID',
                        
                        'INTERNAL_BE_PRODUCT_FAMILY','ADJUSTED_CATEGORY','Contract Type',
                        'Part Number Example','Description ','Convertsto','SNTC SSPT Offer Flag',
                        'Current SSPT Flag','Comments','Product SKU','L12HR','L12OS','L14HR',
                        'L14OS','L1DCP','L1DS','L1NB3','L1NB5','L1NBD','L1NCD','L1NCO','L1NOS',
                        'L1SWT','L22HR','L22OS','L24H3','L24H5','L24HR','L24OS','L2DCP','L2DS',
                        'L2NB3','L2NB5','L2NBD','L2NCD','L2NCO','L2NOS','L2SWT']

            tac_df = pd.DataFrame(df, columns=tac_columns)

            types = tac_df.dtypes.to_dict()

            for t in types:
                if types[t] == "int64":
                    tac_df[t] = tac_df[t].fillna(0)
                elif types[t] == "float64":
                    tac_df[t] = tac_df[t].fillna(0.0)
                else:
                    tac_df[t] = tac_df[t].fillna('')

            tac_df[['CURRENT_SEVERITY_INT', 'MAX_SEVERITY_INT']] = tac_df[[
                'CURRENT_SEVERITY_INT', 'MAX_SEVERITY_INT']].astype('int32')
            tac_df[['SR_TIME_TO_CLOSE_DAYS_CNT']] = tac_df[[
                'SR_TIME_TO_CLOSE_DAYS_CNT']].astype('float')
            tac_df = tac_df.drop(columns=["INCIDENT_ID"])
            
            dfs.append(tac_df)
            
            if len(df) <= 0:
                print(f'No {id_type} data')
                continue
    
         
    tac_df_final = pd.concat(dfs)      

    print(f"{number}) TAC data downloaded!")
    return tac_df_final

#---------------------------------------------------------EA----------------------------------------------------------

def get_EA_details_data_(user, savs, gus, parties, cavs, cs, number):

    """Get Coverage data from Snowflake by given Identifier type

    param: user - cisco e-mail address
    param: sav_ids - list of given sav ids"""
    if cs.is_closed():
        cs,cnn=connec_to_sf(user)

    dfs = []
        
    types_list = {'SAV': savs, 'GU': gus, 'CR': parties, 'CAV': cavs}

    for id_type in types_list.keys():

        ids = types_list.get(id_type)

        if ids == '':
            continue
        
        else:
            query_EA_details = f"""SELECT CUSTOMER_ID AS "ACCOUNT_ID",
                                CUSTOMER_NAME AS "ACCOUNT_NAME",
                                BK_BUSINESS_ENTITY_NAME AS "ARCHITECTURE",
                                BK_SUB_BUSINESS_ENTITY_NAME AS "SUB_ARCHITECTURE",
                                BK_PRODUCT_ID AS "PRODUCT_ID",
                                PRODUCT_FAMILY_ID AS "PRODUCT_FAMILY",
                                DV_GOODS_PRODUCT_CATEGORY_CD AS "PRODUCT_CATEGORY",
                                SUM(SSPT_PRICE) AS "SSPT_LIST_PRICE",
                                SUM(QUANTITY) AS "ITEM_QTY",
                                SUM(LIST_PRICE) AS LIST_PRICE,
                                ACCT_IDENTIFIER AS "ACCOUNT_IDENTIFIER"

                                FROM "CX_DB"."CX_CA_BR"."BV_EA"
                                WHERE
                                CUSTOMER_ID in  ({ids})
                                AND ACCT_IDENTIFIER='{id_type}'
                                group by 1,2,3,4,5,6,7,11
                         """

            cs.execute(query_EA_details)
            df = cs.fetchall()
            #cs.close()
            #cnn.close()
            
            EA_details_columns = ['ACCOUNT_ID','ACCOUNT_NAME','ARCHITECTURE','SUB_ARCHITECTURE','PRODUCT_ID','PRODUCT_FAMILY','PRODUCT_CATEGORY','SSPT_LIST_PRICE','ITEM_QTY','LIST_PRICE','ACCOUNT_IDENTIFIER']
            EA_details_df = pd.DataFrame(df, columns=EA_details_columns)

            if len(EA_details_df) <= 0:
                print(f'No {id_type} data')
                continue

            dfs.append(EA_details_df)
    ea_df_final = pd.concat(dfs)
    print(f"{number}) EA details data downloaded!")
    return ea_df_final

def get_EA_eligibility_data_(user, savs, gus, parties, cavs, cs, number):

    """Get Coverage data from Snowflake by given Identifier type

    param: user - cisco e-mail address
    param: sav_ids - list of given sav ids"""
    if cs.is_closed():
        cs,cnn=connec_to_sf(user)

    dfs = []
        
    types_list = {'SAV': savs, 'GU': gus, 'CR': parties, 'CAV': cavs}

    #print("Downloading EA eligibility data...")
    for id_type in types_list.keys():
        ids = types_list.get(id_type)
        
        if ids == '':
            continue
        else:
            query_EA_eligibility = f"""
                                SELECT CUSTOMER_ID AS "ACCOUNT_ID",
                                CUSTOMER_NAME AS "ACCOUNT_NAME",
                                BK_BUSINESS_ENTITY_NAME AS ARCHITECTURE,
                                EXISTING_EA_CUST_ID_FLAG,
                                EXISTING_EA_CUST_NAME_FLAG,
                                EA_MIG_ELIG_CUST_ID_FLAG,
                                EA_MIG_ELIG_CUST_NAME_FLAG,
                                SUM(SSPT_PRICE) AS "SSPT_LIST_PRICE",
                                SUM(QUANTITY) AS "ITEM_QTY",
                                ACCT_IDENTIFIER AS "ACCOUNT_IDENTIFIER"
                                FROM "CX_DB"."CX_CA_BR"."BV_EA"
                                WHERE
                                CUSTOMER_ID in ({ids})
                                AND ACCT_IDENTIFIER = '{id_type}'
                                GROUP BY 1,2,3,4,5,6,7,10
                         """
            cs.execute(query_EA_eligibility)
            df = cs.fetchall()
            #cs.close()
            #cnn.close()
            
            EA_eligibility_columns = ['ACCOUNT_ID','ACCOUNT_NAME','ARCHITECTURE','EXISTING_EA_CUST_ID_FLAG','EXISTING_EA_CUST_NAME_FLAG','EA_MIG_ELIG_CUST_ID_FLAG','EA_MIG_ELIG_CUST_NAME_FLAG',"SSPT_LIST_PRICE","ITEM_QTY",'ACCOUNT_IDENTIFIER']
            EA_eligibility_df = pd.DataFrame(df, columns=EA_eligibility_columns)
            
            if len(EA_eligibility_df) <= 0:
                print(f'No {id_type} data')
                continue
            
            dfs.append(EA_eligibility_df)

    ea_df_final = pd.concat(dfs)
    print(f"{number}) EA eligibility data downloaded!")
    
    return ea_df_final
    #---------------------------------------------------------------------------------------------------------------------


def get_cav_names(user, ids):
    """Get CAV Names from CAV IDs

    param: user - cisco e-mail address
    param: cav_ids - list of given cav ids"""

    if ids == '':
        ids = 0

    cs=connec_to_sf(user)

    query_cav_names = f"""SELECT "CAV_ID", "CAV_NAME" FROM "CX_DB"."CX_CA_BR"."ACCOUNT_ID_LOOKUP"
        WHERE "CAV_ID" IN ({ids})
        GROUP BY "CAV_ID","CAV_NAME"
        """

    cs.execute(query_cav_names)
    df = cs.fetchall()
    cs.close()

    cav_columns = ['CAV ID', 'CAV NAME']

    cav_df = pd.DataFrame(df, columns=cav_columns)

    types = cav_df.dtypes.to_dict()

    for t in types:
        if types[t] == "int64":
            cav_df[t] = cav_df[t].fillna(0)
        elif types[t] == "float64":
            cav_df[t] = cav_df[t].fillna(0.0)
        else:
            cav_df[t] = cav_df[t].fillna('')

    return cav_df


def get_schema(table, id_type=None):

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
        TableDefinition.Column(
            'Default Service List Price USD', SqlType.double()),
        TableDefinition.Column('Item Quantity', SqlType.double()),
        TableDefinition.Column(
            'Annual Extended Contract Line List USD Amount', SqlType.double()),
        TableDefinition.Column(
            'Annual Contract Line Net USD Amount', SqlType.double()),
        TableDefinition.Column(
            'Annualized Extended Contract Line List USD Amount', SqlType.double()),
        TableDefinition.Column(
            'Annualized Contract Line Net USD Amount', SqlType.double()),
        TableDefinition.Column(
            'Contract Line List Price USD', SqlType.double()),
        TableDefinition.Column(
            'Contract Line Net Price USD', SqlType.double()),
        TableDefinition.Column('Asset List Amount', SqlType.double())
    ]

    coverage_cols = [
        TableDefinition.Column('ID', SqlType.text(), NOT_NULLABLE),
        TableDefinition.Column('Name', SqlType.text(), NOT_NULLABLE),
        TableDefinition.Column('Coverage', SqlType.text(), NOT_NULLABLE),
        TableDefinition.Column('Item Quantity', SqlType.int(), NOT_NULLABLE),
        TableDefinition.Column('Asset List Amount',
                               SqlType.double(), NOT_NULLABLE),
        TableDefinition.Column(
            'Asset Net Amount', SqlType.double(), NOT_NULLABLE),
        TableDefinition.Column(
            'Annual Contract Line Net USD Amount', SqlType.double(), NOT_NULLABLE),
        TableDefinition.Column('Contract Line Net Price USD',
                               SqlType.double(), NOT_NULLABLE),
        TableDefinition.Column(
            'Annualized Extended Contract Line List USD Amount', SqlType.double(), NOT_NULLABLE)
    ]

    sw_cols = [
        TableDefinition.Column('ID', SqlType.text(), NOT_NULLABLE),
        TableDefinition.Column('Name', SqlType.text(), NOT_NULLABLE),
        TableDefinition.Column('Sales Level 1 Name',
                               SqlType.text(), NOT_NULLABLE),
        TableDefinition.Column('Sales Level 2 Name',
                               SqlType.text(), NOT_NULLABLE),
        TableDefinition.Column('Subscription Reference ID', SqlType.text()),
        TableDefinition.Column('Subscription Type TAV', SqlType.text()),
        TableDefinition.Column('Asset Type', SqlType.text()),
        TableDefinition.Column('Product Family', SqlType.text()),
        TableDefinition.Column('Product ID', SqlType.text()),
        TableDefinition.Column('Product Description', SqlType.text()),
        TableDefinition.Column('Business Entity', SqlType.text()),
        TableDefinition.Column('Business Sub Entity', SqlType.text()),
        TableDefinition.Column('Contract Term End Quarter', SqlType.int()),
        TableDefinition.Column(
            'Contract Term End Quarter Name', SqlType.text()),
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
        TableDefinition.Column(
            'INCDT_CRET_FISCAL_WEEK_NUM_INT', SqlType.int()),
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
        TableDefinition.Column(
            'CURRENT_ENTITLEMENT_TYPE_NAME', SqlType.text()),
        TableDefinition.Column('INCIDENT_CONTRACT_STATUS', SqlType.text()),
        TableDefinition.Column('CONTRACT NUMBER', SqlType.text()),
        TableDefinition.Column('CONTRACT_TYPE', SqlType.text()),
        TableDefinition.Column(
            'CREATION_CONTRACT_SVC_LINE_ID', SqlType.double()),
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
        TableDefinition.Column('Data Extracted Date', SqlType.date()),
        TableDefinition.Column('RU_BK_PRODUCT_FAMILY_ID', SqlType.text())
    ]

    tac_cols2 = [
        TableDefinition.Column('INCIDENT_NUMBER', SqlType.int()),
        TableDefinition.Column('CURRENT_SEVERITY_INT', SqlType.int()),
        TableDefinition.Column('MAX_SEVERITY_INT', SqlType.int()),
        TableDefinition.Column('INCDT_CREATION_DATE', SqlType.date()),
        TableDefinition.Column(
            'INCDT_CRET_FISCAL_WEEK_NUM_INT', SqlType.int()),
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
        TableDefinition.Column(
            'CURRENT_ENTITLEMENT_TYPE_NAME', SqlType.text()),
        TableDefinition.Column('INCIDENT_CONTRACT_STATUS', SqlType.text()),
        TableDefinition.Column('CONTRACT NUMBER', SqlType.text()),
        TableDefinition.Column('CONTRACT_TYPE', SqlType.text()),
        TableDefinition.Column(
            'CREATION_CONTRACT_SVC_LINE_ID', SqlType.double()),
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
        # Who should be notified on completion of Analysis
        TableDefinition.Column('Parties Active Collectors', SqlType.text()),
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
            tac_cols.pop(70), tac_cols.pop(66)
            tac_cols.insert(69, TableDefinition.Column(
                'GLOBAL_ULTIMATE_ID', SqlType.int()))
        else:
            pass
        return tac_cols
    elif table == 'tac2':
        return tac_cols2
    elif table == 'smartsheet':
        return smartsheet_cols



#LDos Flag function

def LDoS_flag(data):
    list = []
    for x in data:
        if x >= datetime.today():
            list.append('N')
        else:
            list.append('Y')
    return list


# Function for opportunity value of Smartnet Service

def SNTC_Oppty(df, sntc_mapping):

    try:
        sntc_mapping = sntc_mapping.rename(
            columns={"Product SKU": "Product SKU "})
    except:
        pass
    sntc_mapping = sntc_mapping[["Product SKU ", "SNT"]]

    ib = df.copy()
    #ib['Item Quantity']=ib['Item Quantity'].astype(int)

    # Merging mapping and IB
    merge = ib.merge(sntc_mapping, right_on="Product SKU ",
                     left_on="Product ID", how="left")

    # Transforming dates to calculate LDoS flag
    merge['LDoS'] = merge['LDoS'].apply(lambda x: str(x).replace('/', '-'))
    merge['LDoS'] = merge['LDoS'].apply(lambda x: str(x).replace('nan', '2100-01-01 00:00:00'))
    merge['LDoS'] = merge['LDoS'].replace('', '2100-01-01 00:00:00')
    fechas = [datetime.strptime(str(i)[:-5], '%m-%d-%Y') if len(i) < 16 else datetime.fromisoformat(str(i)) for i in merge['LDoS']]
    merge['LDoS'] = fechas

    # Calculating LDoS flag column
    merge['LDoS Flag'] = LDoS_flag(merge['LDoS'])

    # Filtering data

    merge = merge[merge['Coverage'] == 'NOT COVERED']

    merge = merge[(merge['Product Type'] != 'APPLIANCE') & (merge['Product Type'] != 'CABLE') & (
        merge['Product Type'] != 'APPSWIND') & (merge['Product Type'] != 'POWER') & (merge['Product Type'] != 'DOC')]

    merge = merge[(merge['LDoS Flag'] == 'N')]

    merge["IB Oppty"] = merge["SNT"]/1000
    sntc_oppty = merge.iloc[:, np.r_[0, -1]].groupby(merge.columns[0]).sum()

    if len(sntc_oppty) > 0:
        return sntc_oppty['IB Oppty'].sum()
    else:
        return 'N/A'



#Function that generates the CSV files

def set_datasource(df,type,folder_path
                   ,contract_types_list,
                   success_track_pricing_list,sspt_pricing_list_eligibleSSPT,
                   sspt_pricing_list_outputTable,sntc_pricing_list,product_banding):

    dataframe = df.copy()
    ## --------------------------------------------- IB
    if type == 'ib':

        dataframe.rename(columns={dataframe.columns[0]:'ID', dataframe.columns[1]:'Name'},inplace=True)

        '''st = success_track_pricing_list[['Product SKU','L12HR']]
        dataframe = dataframe.merge(st, how='left', left_on='Product ID',right_on='Product SKU', suffixes=('', ' (Success Track PIDs)'))'''
        sp = sspt_pricing_list_outputTable[['Product SKU','ELSUS','SSPTS','SSS2P','SSTCM','SSSW']]
        dataframe = dataframe.merge(sp, how='left',left_on='Product ID', right_on='Product SKU', suffixes=('', ' (Output Table)'))

        dataframe.to_csv(folder_path, index=False)

    ## --------------------------------------------- Coverage

    elif type == 'coverage':

        coverage_cols = ['ID', 'Name', 'Coverage', 'Item Quantity', 'Asset List Amount', 'Asset Net Amount', 'Annual Contract Line Net USD Amount',
                        'Contract Line Net Price USD', 'Annualized Extended Contract Line List USD Amount']

        dataframe.columns = coverage_cols

        dataframe = dataframe.astype({
                                        'ID': str,
                                        'Name': str,
                                        'Coverage': str,
                                        'Item Quantity': int,
                                        'Asset List Amount': float,
                                        'Asset Net Amount': float,
                                        'Annual Contract Line Net USD Amount': float,
                                        'Contract Line Net Price USD': float,
                                        'Annualized Extended Contract Line List USD Amount': float
                                    })

        dataframe.to_csv(folder_path, index=False)

    ## --------------------------------------------- SW
    elif type == 'sw':

        sw_cols = [
            'ID', 'Name', 'Sales Level 1 Name', 'Sales Level 2 Name', 'Subscription Reference ID', 'Subscription Type TAV',
            'Asset Type', 'Product Family', 'Product ID', 'Product Description', 'Business Entity', 'Business Sub Entity',
            'Contract Term End Quarter', 'Contract Term End Quarter Name', 'Asset List', 'Asset Net', 'Buying Program',
        ]

        dataframe.columns = sw_cols

        dataframe = dataframe.astype({
                                        'ID': str,
                                        'Name': str,
                                        'Sales Level 1 Name': str,
                                        'Sales Level 2 Name': str,
                                        'Subscription Reference ID': str,
                                        'Subscription Type TAV': str,
                                        'Asset Type': str,
                                        'Product Family': str,
                                        'Product ID': str,
                                        'Product Description': str,
                                        'Business Entity': str,
                                        'Business Sub Entity': str,
                                        'Contract Term End Quarter': int,
                                        'Contract Term End Quarter Name': str,
                                        'Asset List': float,
                                        'Asset Net': float,
                                        'Buying Program': str
                                    })

        dataframe.to_csv(folder_path, index=False)

    ## --------------------------------------------- TAC

    elif type == 'tac':

        tac_cols = [
            'INCIDENT_NUMBER', 'CURRENT_SEVERITY_INT', 'MAX_SEVERITY_INT', 'INCDT_CREATION_DATE', 'INCDT_CRET_FISCAL_WEEK_NUM_INT',
            'SR Creation FY Week', 'SR Creation FY Month', 'SR Creation FY Quarter', 'SR Creation Fiscal Year', 'CLOSED_DATE',
            'INCDT_CLOSED_FISCAL_MONTH_NM', 'INCDT_CLOSED_FISCAL_QTR_NM', 'INCDT_CLOSED_FISCAL_YEAR', 'SR_TIME_TO_CLOSE_DAYS_CNT',
            'CURRENT_SERIAL_NUMBER', 'COMPLEXITY_DESCR', 'INITIAL_PROBLEM_CODE', 'OUTAGE_FLAG', 'ENTRY_CHANNEL_NAME', 'REQUEST_TYPE_NAME',
            'BUG_CNT', 'INCIDENT_STATUS', 'CURRENT_ENTITLEMENT_TYPE_NAME', 'INCIDENT_CONTRACT_STATUS', 'CONTRACT NUMBER', 'CONTRACT_TYPE',
            'CREATION_CONTRACT_SVC_LINE_ID', 'HW_VERSION_ID', 'SW_VERSION_ID', 'TAC_PRODUCT_SW_KEY', 'UPDATED_COT_TECH_KEY', 'INCIDENT_TYPE',
            'PROBLEM_CODE', 'RESOLUTION_CODE', 'PART_NUMBER', 'SOLUTION_CNT', 'ERP_FAMILY', 'ERP_PLATFORM_NAME', 'TAC_PRODUCT_HW_KEY',
            'TAC_HW_PLATFORM_NAME', 'TECH_NAME', 'BUSINESS_UNIT_CODE', 'HYBRID_PRODUCT_FAMILY', 'SUB_TECH_NAME', 'SR_PRODUCT_ID', 'UNDERLYING_CAUSE_NAME',
            'UNDERLYING_CAUSE_DESCRIPTION', 'CASE_NUMBER', 'B2B_FLAG', 'TECH_ID', 'SUB_TECH_ID', 'SW_VERSION_ACT_ID', 'SW_VERSION_NAME', 'VALID_SR_FILTER_FLAG',
            'CUSTOMER_VERTICAL_CD', 'CUSTOMER_MARKET_SEGMENT_CD', 'ISO_COUNTRY_CD', 'Initial Time to Resolution', 'Final Time to Resolution', 'SRC_DEL_FLG',
            'Business Ownership Time', 'Customer Ownership Time', 'Other Ownership Time', 'Delivery Ownership Time', 'CUSTOMER_NAME', 'PARTY ID', 'PARTY NAME', 'ID',
            'SERVICE_SUBGROUP_DESC', 'SERVICE_LEVLE_CODE', 'SERVICE_PROGRAM', 'SERVICE_BRAND', 'SR_TECHNOLOGY', 'SR_SUB_TECHNOLOGY', 'BE_INT', 'SUB_BE_INT',
            'FLAG', 'Data Extracted Date'
        ]

        dataframe.columns = tac_cols

        dataframe = dataframe.astype({
                                        'INCIDENT_NUMBER': int,
                                        'CURRENT_SEVERITY_INT': int,
                                        'MAX_SEVERITY_INT': int,
                                        #'INCDT_CREATION_DATE', SqlType.date()),
                                        'INCDT_CRET_FISCAL_WEEK_NUM_INT': int,
                                        'SR Creation FY Week': str,
                                        'SR Creation FY Month': str,
                                        'SR Creation FY Quarter': str,
                                        'SR Creation Fiscal Year': int,
                                        #'CLOSED_DATE', SqlType.date()),
                                        'INCDT_CLOSED_FISCAL_MONTH_NM': str,
                                        'INCDT_CLOSED_FISCAL_QTR_NM': str,
                                        'INCDT_CLOSED_FISCAL_YEAR': int,
                                        'SR_TIME_TO_CLOSE_DAYS_CNT': float,
                                        'CURRENT_SERIAL_NUMBER': str,
                                        'COMPLEXITY_DESCR': str,
                                        'INITIAL_PROBLEM_CODE': str,
                                        'OUTAGE_FLAG': str,
                                        'ENTRY_CHANNEL_NAME': str,
                                        'REQUEST_TYPE_NAME': str,
                                        'BUG_CNT': float,
                                        #'INCIDENT_CONTACT_EMAIL':str,
                                        'INCIDENT_STATUS': str,
                                        'CURRENT_ENTITLEMENT_TYPE_NAME': str,
                                        'INCIDENT_CONTRACT_STATUS': str,
                                        'CONTRACT NUMBER': str,
                                        'CONTRACT_TYPE': str,
                                        'CREATION_CONTRACT_SVC_LINE_ID': float,
                                        #'CURRENT_OWNER_CCO_ID':str,
                                        'HW_VERSION_ID': int,  # big_int
                                        'SW_VERSION_ID': int,  # big_int
                                        'TAC_PRODUCT_SW_KEY': int,
                                        'UPDATED_COT_TECH_KEY': int,
                                        'INCIDENT_TYPE': str,
                                        'PROBLEM_CODE': str,
                                        'RESOLUTION_CODE': str,
                                        'PART_NUMBER': str,
                                        'SOLUTION_CNT': str,
                                        'ERP_FAMILY': str,
                                        'ERP_PLATFORM_NAME': str,
                                        'TAC_PRODUCT_HW_KEY': int,
                                        'TAC_HW_PLATFORM_NAME': str,
                                        'TECH_NAME': str,
                                        'BUSINESS_UNIT_CODE': str,
                                        'HYBRID_PRODUCT_FAMILY': str,
                                        'SUB_TECH_NAME': str,
                                        'SR_PRODUCT_ID': str,
                                        'UNDERLYING_CAUSE_NAME': str,
                                        'UNDERLYING_CAUSE_DESCRIPTION': str,
                                        'CASE_NUMBER': int,
                                        'B2B_FLAG': str,
                                        'TECH_ID': int,
                                        'SUB_TECH_ID': int,
                                        'SW_VERSION_ACT_ID': float,
                                        'SW_VERSION_NAME': str,
                                        'VALID_SR_FILTER_FLAG': str,
                                        'CUSTOMER_VERTICAL_CD': str,
                                        'CUSTOMER_MARKET_SEGMENT_CD': str,
                                        'ISO_COUNTRY_CD': str,
                                        'Initial Time to Resolution': float,
                                        'Final Time to Resolution': float,
                                        'SRC_DEL_FLG': str,
                                        'Business Ownership Time': float,
                                        'Customer Ownership Time': float,
                                        'Other Ownership Time': float,
                                        'Delivery Ownership Time': float,
                                        'CUSTOMER_NAME': str,
                                        'PARTY ID': int,
                                        'PARTY NAME': str,
                                        'ID': int,
                                        'SERVICE_SUBGROUP_DESC': str,
                                        'SERVICE_LEVLE_CODE': str,
                                        'SERVICE_PROGRAM': str,
                                        'SERVICE_BRAND': str,
                                        'SR_TECHNOLOGY': str,
                                        'SR_SUB_TECHNOLOGY': str,
                                        'BE_INT': str,
                                        'SUB_BE_INT': str,
                                        'FLAG': str,
                                        #'Data Extracted Date', SqlType.date())
                                    })

        dataframe['INCDT_CREATION_DATE'] = dataframe['INCDT_CREATION_DATE'].apply(lambda x: x.strftime('%Y-%m-%d %H:%M:%S') if pd.notna(x) else x)
        dataframe['CLOSED_DATE'] = dataframe['CLOSED_DATE'].apply(lambda x: x.strftime('%Y-%m-%d %H:%M:%S') if pd.notna(x) else x)
        dataframe['Data Extracted Date'] = dataframe['Data Extracted Date'].apply(lambda x: x.strftime('%Y-%m-%d %H:%M:%S') if pd.notna(x) else x)

        dataframe = dataframe.merge(product_banding, how='left', left_on='HYBRID_PRODUCT_FAMILY', right_on='INTERNAL_BE_PRODUCT_FAMILY')
        dataframe = dataframe.merge(contract_types_list, how='left', left_on='CONTRACT_TYPE', right_on='Contract Type')
        dataframe = dataframe.merge(success_track_pricing_list, how='left', left_on='SR_PRODUCT_ID', right_on='Product SKU')

        dataframe.to_csv(folder_path, index=False)

    ## --------------------------------------------- CIR

    elif type == 'cir':

        cir_cols = [
            'Party ID', 'ACTIVE_YORN', 'Customer', 'Equipment Type Description', 'Appliance ID', 'Inventory',
            'Collection Date', 'Imported By', 'Product ID', 'Product Family', 'Business Entity', 'Sub Business Entity',
            'Business Entity Description', 'PF', 'Product Description', 'Equipment Type', 'Product Type', 'Serial Number',
            'Last Date of Support', 'Alert URL', 'Contract Number', 'Contract Status', 'Contract Lines Status', 'Service Program',
            'Contract End Date', 'Contract Line End Date', 'ACCOUNT_ID', 'ID', 'Updated Date'
        ]

        dataframe.columns = cir_cols

        dataframe = dataframe.astype({
                                        'Party ID': int,
                                        'ACTIVE_YORN': str,
                                        'Customer': str,
                                        'Equipment Type Description': str,
                                        'Appliance ID': str,
                                        'Inventory': str,
                                        #'Collection Date', SqlType.date()),
                                        'Imported By': str,
                                        'Product ID': str,
                                        'Product Family': str,
                                        'Business Entity': str,
                                        'Sub Business Entity': str,
                                        'Business Entity Description': str,
                                        'PF': str,
                                        'Product Description': str,
                                        'Equipment Type': float,
                                        'Product Type': str,
                                        'Serial Number': str,
                                        #'Last Date of Support', SqlType.date()),
                                        'Alert URL': str,
                                        'Contract Number': str,
                                        'Contract Status': str,
                                        'Contract Lines Status': str,
                                        'Service Program': str,
                                        #'Contract End Date', SqlType.date()),
                                        #'Contract Line End Date', SqlType.date()),
                                        'ACCOUNT_ID': str,
                                        'ID': float,
                                        #'Updated Date', SqlType.date())
                                    })

        dataframe['Collection Date'] = dataframe['Collection Date'].apply(lambda x: x.strftime('%Y-%m-%d %H:%M:%S') if pd.notna(x) else x)
        dataframe['Last Date of Support'] = dataframe['Last Date of Support'].apply(lambda x: x.strftime('%Y-%m-%d %H:%M:%S') if pd.notna(x) else x)
        dataframe['Contract End Date'] = dataframe['Contract End Date'].apply(lambda x: x.strftime('%Y-%m-%d %H:%M:%S') if pd.notna(x) else x)
        dataframe['Contract Line End Date'] = dataframe['Contract Line End Date'].apply(lambda x: x.strftime('%Y-%m-%d %H:%M:%S') if pd.notna(x) else x)
        dataframe['Updated Date'] = dataframe['Updated Date'].apply(lambda x: x.strftime('%Y-%m-%d %H:%M:%S') if pd.notna(x) else x)

        dataframe = dataframe.merge(success_track_pricing_list, how='left', left_on='Product ID', right_on='Product SKU', suffixes=('', ' (Success Track PIDs)'))
        dataframe = dataframe.merge(sspt_pricing_list_eligibleSSPT, how='left', left_on='Product ID', right_on='Product SKU', suffixes=('', ' (Eligible SSPT)'))
        dataframe = dataframe.merge(sspt_pricing_list_outputTable, how='left', left_on='Product ID', right_on='Product SKU', suffixes=('', ' (Output Table)'))
        dataframe = dataframe.merge(sntc_pricing_list, how='left', left_on='Product ID', right_on='Product SKU ', suffixes=('', ' (Output)'))
        dataframe = dataframe.merge(product_banding, how='left', left_on='Product Family', right_on='INTERNAL_BE_PRODUCT_FAMILY')
        
        dataframe.to_csv(folder_path, index=False)

    return dataframe


#Calculation of Solution Support recommenden SL oppty
    
def SSPT_Oppty(IB):

    #Leyendo archivo de IB
    merge = IB.copy()

    if len(merge) == 0:
        return 'N/A'
    else:

        #Transformando las fechas para calcular la columna LDos flag
        merge['LDoS']=merge['LDoS'].apply(lambda x: str(x).replace('/','-'))    
        merge['LDoS']=merge['LDoS'].apply(lambda x: str(x).replace('NaT', '2100-01-01 00:00:00'))
        merge['LDoS']=merge['LDoS'].apply(lambda x: str(x).replace('None', '2100-01-01 00:00:00'))
        fechas = [datetime.strptime(str(i)[:-5],'%m-%d-%Y') if len(i)<16 else datetime.fromisoformat(str(i)) for i in merge['LDoS']]
        merge['LDoS'] = fechas

        #Calculando columna LDos flag
        merge['LDoS Flag']=''
        merge['LDoS Flag']= LDoS_flag( merge['LDoS'])
        
        def select_data_to_display(LDoS_flag,cover_lin_status,eligible,coverage):
            value = ''
            if (LDoS_flag =='Y') & (cover_lin_status != 'TERMINATED') & (eligible=='Y') & (coverage=='NOT COVERED'):
                value='Uncovered Opportunity'
            elif  (eligible=='Y') & (coverage=='COVERED'):
                value='Covered Opportunity'
            else:
                value = 'Uncovered IB to be Excluded'

            return value

        merge['Select data to Display'] = merge[['LDoS Flag','Covered Line Status','Eligible','Coverage']].apply(lambda x: select_data_to_display(x[0],x[1],x[2],x[3]),axis=1)

        #Ejecutando los filtros a la tabla


        merge2=merge[(merge['Select data to Display'] == 'Covered Opportunity')]
        merge3=merge[(merge['Select data to Display'] == 'Uncovered Opportunity')]

        merge=pd.concat([merge2,merge3])

        merge=merge[merge['Coverage']=='COVERED']

        #merge4=merge[(merge['SNTC SSPT Offer Flag']== 'CISCO SWSS')]
        merge5=merge[(merge['SNTC SSPT Offer Flag']=='Combined Services')]
        merge6=merge[(merge['SNTC SSPT Offer Flag']=='Partner Support')]
        merge7=merge[(merge['SNTC SSPT Offer Flag']=='Smartnet Total Care')]
        merge8=merge[(merge['SNTC SSPT Offer Flag']=='SP Base')]
        #merge9=merge[(merge['SNTC SSPT Offer Flag']=='SS PLUS SWSS')] 
        merge11=merge[(merge['SNTC SSPT Offer Flag']=='TELEPRESENCE CUSTOMERS')]

        merge=pd.concat([merge5,merge6,merge7,merge8,merge11])

        #filtro product banding
        merge12=merge[merge["ADJUSTED_CATEGORY"]=='High']
        merge13=merge[merge["ADJUSTED_CATEGORY"]=='Mid']
        
        merge=pd.concat([merge12,merge13])

        #filtro upsell to sspt
        merge=merge[pd.notna(merge['Convertsto'])]

        #Success Track: All
        #Arquitecture: All
        #Product Family: All

        merge = merge.reset_index()

        # SSPT contract filter
        merge = Uplift_Recommended_SL(merge)

        
        merge = merge[(merge['Uplift sspt'] - merge['Annualized Extended Contract Line List USD Amount'])>=0]

        merge2 = merge

        SP_Oppty1 = (merge['Uplift sspt'] - merge2['Annualized Extended Contract Line List USD Amount']).sum()

        return round(SP_Oppty1,1)

#Calculation of Success Tracks oppty

def ST_Oppty(IB):
    '''
    Def:
        Calculates the Success Tracks opportunity.
    Inputs:
        df = dataframe containing the IB
        st_mapping = Success tracks price list
    Outs:
        ST_Oppty value in KUSD
    '''
    #Loading necesary files
    merge1 = IB.copy()

    #------------------------------------------------Calculating Success Tracks Column-------------------------------------------------
    merge1['Product SKU'] = merge1['Product ID']
    merge1 = merge1.rename(columns={'Product SKU':'Success Tracks'})
    merge1['Success Tracks'] = merge1['Success Tracks'].fillna('Not Elegible')
    merge1['Item Quantity'].fillna(0, inplace=True)

    if len(merge1) == 0:
        return 'N/A'
    else:
        j=merge1.iloc[0,0]  
        #------------------------------------------------------------------------s-----------------------------------------
        # if contract type is in list_st mark it as 'Not Elegible'
        lista_st = ['L1NB3','L1NB5','L1NBD','L1SWT','L24H3','L24H5','L24HR','L2NB3','L2NB5','L2NBD','L2SWT']
        merge1['Success Tracks'].astype(str) 
        merge1['Success Tracks'] = merge1[['Success Tracks','Contract Type']].apply(lambda x: 'Not Elegible' if x[1] in lista_st else x[0], axis=1)

        #------------------------------------------------------------------------------------------------------------------
        #Else mark it as 'Elegible'
        merge1['Success Tracks'] = merge1['Success Tracks'].apply(lambda x: 'Elegible' if x != 'Not Elegible' else x)

        #-----------------------------------------------Calculating SmartNet NBD ST Eligible-----------------------------------------------
        #If the product it's elegible use the annualized extended contract line list USD Amount, otherwise it's 0
        merge1['SmartNet NBD ST Eligible'] = merge1['Annualized Extended Contract Line List USD Amount']      
        merge1['SmartNet NBD ST Eligible'] = merge1[['SmartNet NBD ST Eligible','Success Tracks']].apply(lambda x: 0 if x[1] != 'Elegible' else x[0], axis=1)

        #-----------------------------------------------Calculating DNA HW Appliance-----------------------------------------------
        #Check if 'DN2-HW-APL' or 'DN1-HW-APL' is in product ID and mark it as 1
        merge1['DNA HW Appliance'] = merge1['Product ID']
        merge1['DNA HW Appliance'] = merge1[['Product ID','DNA HW Appliance']].apply(lambda x: '1' if ('DN2-HW-APL' in x[0]) or ('DN1-HW-APL' in x[0]) else x[1], axis=1)

        # If not mark it as 0
        merge1['DNA HW Appliance'] = merge1['DNA HW Appliance'].apply(lambda x: '0' if x != '1' else x)

        #-----------------------------------------------Calculating SmartNet ST Not Covered-----------------------------------------------
        # If it's not covered and 1 in DNA HW Appliance get the annualized extended contract line list USD Amount
        merge1['SmartNet ST Not Covered'] = 0
        merge1['SmartNet ST Not Covered'] = merge1[['Coverage','DNA HW Appliance','SmartNet ST Not Covered','Default Service List Price USD']].apply(lambda x: x[3] if (x[0] == 'NOT COVERED') and (x[1] == '1') else x[2], axis=1)

        #-----------------------------------------------Calculating ST Estimated List Price-----------------------------------------------
        
        merge1['L2 SWT'] = merge1['L2SWT'] # *merge1['Item Quantity']
        merge1['Estimated L2 Scv'] = merge1['L2NBD']+merge1['L2 SWT']
        merge1['ST Estimated List Price'] = merge1['Estimated L2 Scv'] # * merge1['Item Quantity']
        
        #------------------------------------------------------Filtering------------------------------------------------------------------
        merge1 = merge1[merge1['Coverage'] == 'COVERED']
        merge1 = merge1[merge1['Success Tracks'] == 'Elegible']

        #filtro product banding
        merge1=merge1[merge1["ADJUSTED_CATEGORY"].isin(['High','Mid'])]

        merge1['Contract type filter'] = merge1['ST Estimated List Price'] - merge1['Annualized Extended Contract Line List USD Amount']
        merge1['Contract type filter'] = merge1['Contract type filter'].apply(lambda x: 'False' if x < 0 else 'True')
        merge1 = merge1[merge1['Contract type filter']=='True']

        #----------------------------------------Calculating Total ST Oppty KPI------------------------------------------------------------
        merge1 = merge1[(merge1['ST Estimated List Price']-merge1['Annualized Extended Contract Line List USD Amount'])>=0]

        sn_nbd_st_e = merge1['SmartNet NBD ST Eligible'].sum()
        sn_st_not_covered = merge1['SmartNet ST Not Covered'].sum()
        st_e_list_price = merge1['ST Estimated List Price'].sum()
        ST_Oppty1= round (((st_e_list_price - sn_st_not_covered - sn_nbd_st_e)),1)
       

        return  ST_Oppty1


#Calculation of smartnet value

def smartnet_verification(ib):
    df = ib.copy()
    oppty = df.groupby('OE_KEY')[['DEFAULT_SERVICE_LIST_PRICE_USD']].sum()
    oppty = oppty.reset_index()
    return oppty


#Calculation of IB value, IB covered and Mayor Renewal 

def IB_attributes(IB, Coverage):
    
# ------------------------------------------------------------------Calculating IB-----------------------------------------------------------------------------------
    data = Coverage.groupby([Coverage['OE_KEY'],Coverage['COVERAGE'][Coverage['COVERAGE']=='COVERED']])['ASSET_LIST_AMOUNT'].sum()
    #data = IB.groupby([IB['ID'],IB['coverage_filtered'][IB['coverage_filtered']=='COVERED']])[['Asset List Amount']].sum()
    data = data.reset_index()
    data = data.rename(columns={'ASSET_LIST_AMOUNT':'IB Value'})
    data = data.drop(labels='COVERAGE', axis=1)
    data.fillna(0,inplace=True)
    

# ------------------------------------------------------------------Calculating % coverage------------------------------------------------------------------------------
    a = Coverage.groupby([Coverage['OE_KEY']])[['ASSET_LIST_AMOUNT']].sum()
    a = a.reset_index()
    a = a.rename(columns={'ASSET_LIST_AMOUNT':'IB Total'})
    data = data.merge(a,right_on="OE_KEY",left_on="OE_KEY", how='left')
    data.fillna(0,inplace=True)
    data['Coverage Percentage'] = (data['IB Value']/data['IB Total'])*100
    data = data.drop(labels='IB Total', axis=1)
    

#---------------------------------------------------------------Calculating Mayor Renewal -----------------------------------------------------------------------------

    IB['ANNUAL_EXTENDED_CONTRACT_LINE_LIST_USD_AMOUNT'] = IB['ANNUAL_EXTENDED_CONTRACT_LINE_LIST_USD_AMOUNT'].astype(float) 
    IB['OE_KEY'] = IB['OE_KEY'].astype(str) 

    g = IB.groupby([IB['OE_KEY'],IB['CONTRACT_LINE_END_QUARTER']])['ANNUAL_EXTENDED_CONTRACT_LINE_LIST_USD_AMOUNT'].sum()
    g = g.reset_index()
    g['CONTRACT_LINE_END_QUARTER'] = g['CONTRACT_LINE_END_QUARTER'].astype(str)
    h = g.groupby(g['OE_KEY'])[['ANNUAL_EXTENDED_CONTRACT_LINE_LIST_USD_AMOUNT']].max()

    lis = h['ANNUAL_EXTENDED_CONTRACT_LINE_LIST_USD_AMOUNT'].to_list()
    lis2 = []
    for i in lis:
        j = g[g['ANNUAL_EXTENDED_CONTRACT_LINE_LIST_USD_AMOUNT'] == i].iloc[0,1]
        lis2.append(j)

    #print(f'Tipo de ID IB {data['ID'].apply(type))}')

    df_mayor_ren=h.reset_index()
    df_mayor_ren['Mayor Renewal'] = lis2
    df_mayor_ren = df_mayor_ren.drop(labels='ANNUAL_EXTENDED_CONTRACT_LINE_LIST_USD_AMOUNT', axis=1)
    data= data.merge(df_mayor_ren,right_on="OE_KEY",left_on="OE_KEY", how='left')
    data.fillna(0,inplace=True)

    return data


#Assign color function for Q&A dataframe

def color_qa(value):
    if value in ['Incorrect', 'Negative Value','Empty Value', 'Big value','QA Package Info', 'No IB data', 'No data']:
        color = 'red'
    else:
        color = 'green'

    return 'color: %s' % color


def ib_values_validation(data):
    ib_validated = data.copy()
    
    ib_validated['IB Value'] = ib_validated['IB Value'].apply(lambda x: 'Incorrect' if x < 0 else 'Correct')
    ib_validated['Coverage Percentage'] = ib_validated['Coverage Percentage'].apply(lambda x: 'Correct' if (x/100 >= 0) & (x/100 <= 1) else 'Incorect')
    ib_validated['Mayor Renewal'] = ib_validated['Mayor Renewal'].apply(lambda x: 'Empty Value' if x == '' else 'Correct')
    
    return ib_validated

#Validation for negative values 

def oppty_validation(oppty):
    
    oppty_validated = oppty.copy()
    oppty_validated['DEFAULT_SERVICE_LIST_PRICE_USD'] = oppty_validated['DEFAULT_SERVICE_LIST_PRICE_USD'].apply(lambda x: 'Correct' if x >= 0 else 'Negative Value')
    oppty_validated = oppty_validated.rename(columns={'DEFAULT_SERVICE_LIST_PRICE_USD':'Smartnet validation'})
    return oppty_validated

#Calculation of smartnet value for PI's eligibles for Success Tracks

def smartnet_total_care_NBD_list_price(df):
    
    ib = df.copy()
    ib['Contract type filter'] = ib['SUCCESS_TRACKS_PRICING_L2'].apply(float) - ib['ANNUALIZED_EXTENDED_CONTRACT_LINE_LIST_USD_AMOUNT']
    ib = ib[ib['Contract type filter'] > 0]
    
    ib = ib[ib['ADJUSTED_CATEGORY'].isin(['High','Mid'])]
    ib = ib[ib['COVERAGE'] == 'COVERED']
    ib = ib = ib[~(pd.isna(ib['PRODUCT_ID']) | ib['CONTRACT_TYPE'].isin(['L1NB3','L1NB5','L1NBD','L1SWT','L24H3','L24H5','L24HR','L2NB3','L2NB5','L2NBD','L2SWT']) | (ib['SUCCESS_TRACK_YORN'] == 'N'))]
    
    smartnet = ib.groupby('OE_KEY')[['ANNUALIZED_EXTENDED_CONTRACT_LINE_LIST_USD_AMOUNT']].sum().round().reset_index()
    smartnet['Lenght'] = smartnet['ANNUALIZED_EXTENDED_CONTRACT_LINE_LIST_USD_AMOUNT'].apply(lambda x: len(str(x).split('.')[0]) )
    
    return smartnet



#Calculation of ST level 2 opportunity 

def estimated_list_price(df):

    ib = df.copy()
    #Current Contract Type Filter
    ib['Contract type filter'] = ib['SUCCESS_TRACKS_PRICING_L2'].apply(float) - ib['ANNUALIZED_EXTENDED_CONTRACT_LINE_LIST_USD_AMOUNT']
    ib = ib[ib['Contract type filter'] > 0]

    ib = ib[ib['ADJUSTED_CATEGORY'].isin(['High','Mid'])]
    ib = ib[ib['COVERAGE'] == 'COVERED']
    ib = ib = ib[~(pd.isna(ib['PRODUCT_ID']) | ib['CONTRACT_TYPE'].isin(['L1NB3','L1NB5','L1NBD','L1SWT','L24H3','L24H5','L24HR','L2NB3','L2NB5','L2NBD','L2SWT']) | (ib['SUCCESS_TRACK_YORN'] == 'N'))]
    
    estimated_value = ib.groupby('OE_KEY')[['SUCCESS_TRACKS_PRICING_L2']].sum().round().reset_index()
    estimated_value['Lenght'] = estimated_value['SUCCESS_TRACKS_PRICING_L2'].apply(lambda x: len(str(x).split('.')[0]) )
    
    return estimated_value


#Validation of lenght for calculated values

def lenght_validation(df):
    l_validation = df.copy()
    l_validation['Lenght'] = l_validation['Lenght'].apply(lambda x: 'Big value' if x >= 8 else 'Correct')
    
    return l_validation[['OE_KEY','Lenght']]


def smartsheet_len_info(df):
    
    parties = df['Who should be notified on completion of Analysis'].iloc[0].split(',')
    appliance = df['Appliance ID'].iloc[0].split(',')
    inventory = df['Inventory Name'].iloc[0].split(',')
    
    if (len(df['sav_list'].iloc[0]) > 11)  | (len(df['gu_list'].iloc[0]) > 11) | (len(df['cav_list'].iloc[0]) > 11) \
        |(len(df['contract_list'].iloc[0]) > 11) | (len(parties) > 11) | (len(appliance) > 11) | (len(inventory) > 11):
        return 'QA Package Info'
    else:
        return 'Correct'
    
# Function for the calculus of the sspt uplift opportunity

def Uplift_Recommended_SL(df):

    ib_df = df.copy()
    if len(ib_df)>0:
        ib_df['Uplift sspt'] = pd.NaT
        ib_df['Uplift sspt'] = ib_df[['Contract Type','SSC2P','SSSNT','Uplift sspt']].apply(lambda x: (x[1] if pd.notna(x[1]) else x[2]) if x[0] == 'C2P' else x[3], axis=1)
        ib_df['Uplift sspt'] = ib_df[['Contract Type','SSC4P','SSSNT','Uplift sspt']].apply(lambda x: (x[1] if pd.notna(x[1]) else x[2]) if x[0] in ['3C4P','C4P','C4S'] else x[3], axis=1)
        ib_df['Uplift sspt'] = ib_df[['Contract Type','SSCS','SSSNT','Uplift sspt']].apply(lambda x: (x[1] if pd.notna(x[1]) else x[2]) if x[0] == 'CS' else x[3], axis=1)
        ib_df['Uplift sspt'] = ib_df[['Contract Type','SSDR5','SSSNT','Uplift sspt']].apply(lambda x: (x[1] if pd.notna(x[1]) else x[2]) if x[0] == 'UCSD5' else x[3], axis=1)
        ib_df['Uplift sspt'] = ib_df[['Contract Type','SSDR7','SSSNT','Uplift sspt']].apply(lambda x: (x[1] if pd.notna(x[1]) else x[2]) if x[0] == 'UCSD7' else x[3], axis=1)
        ib_df['Uplift sspt'] = ib_df[['Contract Type','SSS2P','SSSNT','Uplift sspt']].apply(lambda x: (x[1] if pd.notna(x[1]) else x[2]) if x[0] == 'S2P' else x[3], axis=1)
        ib_df['Uplift sspt'] = ib_df[['Contract Type','SSSNE','SSSNT','Uplift sspt']].apply(lambda x: (x[1] if pd.notna(x[1]) else x[2]) if x[0] == 'SNTE' else x[3], axis=1)
        ib_df['Uplift sspt'] = ib_df[['Contract Type','SSSNP','Uplift sspt']].apply(lambda x: (x[1] if pd.notna(x[1]) else 0) if x[0] in ['3SNTP','5SNTP'] else x[2], axis=1)
        ib_df['Uplift sspt'] = ib_df[['Contract Type','SSSNP','SSSNT','Uplift sspt']].apply(lambda x: (x[1] if pd.notna(x[1]) else x[2]) if x[0] in ['SNTP'] else x[3], axis=1)
        ib_df['Uplift sspt'] = ib_df[['Contract Type','SSSNT','Uplift sspt']].apply(lambda x: (x[1] if pd.notna(x[1]) else 0) if x[0] in ['3SNT','5SNT','SNT'] else x[2], axis=1)
        ib_df['Uplift sspt'] = ib_df[['Contract Type','SSSNT','SSSW','Uplift sspt']].apply(lambda x: (x[1] if pd.notna(x[1]) else x[2]) if x[0] in ['SW'] else x[3], axis=1)
        ib_df['Uplift sspt'] = ib_df[['Contract Type','ECMUS','Uplift sspt']].apply(lambda x: (x[1] if pd.notna(x[1]) else 0) if x[0] in ['ECMU'] else x[2], axis=1)

        def cs_multiplier_sl(service_level, Sss2P, Ssc2P, Sssnp, Ssc4P, Sssne, Ssc4S, Sssnt, Sscs, Ssdr7, Ssdr5, Sssw):
            if service_level == '24x7x2':
                return Sss2P
            elif service_level == '24x7x2OS': 
                return Ssc2P
            elif service_level == '24x7x4': 
                return Sssnp
            elif service_level == '24x7x4OS': 
                return Ssc4P
            elif service_level == '8x5x4': 
                return Sssne
            elif service_level == '8x5x4OS': 
                return Ssc4S
            elif service_level == 'NBD': 
                return Sssnt
            elif service_level == '8x5xNDBOS': 
                return Sscs
            elif service_level == 'DR 24x7x4OS': 
                return Ssdr7
            elif service_level == 'DR 8x5xNDBOS': 
                return Ssdr5
            elif service_level == 'SNTC NO RMA': 
                return Sssw
            else: 
                return 0

        ib_df['cs multiplier sl'] = ib_df[['Service Level', 'SSS2P', 'SSC2P', 'SSSNP', 'SSC4P', 'SSSNE', 'SSC4S', 'SSSNT', 'SSCS', 'SSDR7', 'SSDR5', 'SSSW']].apply(lambda x: cs_multiplier_sl(x[0],x[1],x[2],x[3],x[4],x[5],x[6],x[7],x[8],x[9],x[10],x[11]), axis=1)
        ib_df['CS SSPT (Multiplier)'] = ib_df[['Uplift','cs multiplier sl','Multiplier']].apply(lambda x: x[1]*x[2] if pd.notna(x[0]) else 0, axis=1)

        ib_df['Uplift sspt'] = ib_df[['SNTC SSPT Offer Flag','CS SSPT (Multiplier)','Uplift sspt']].apply(lambda x: (x[1] if pd.notna(x[1]) else 0) if x[0] in ['Combined Services'] else x[2], axis=1)
        ib_df['Uplift sspt'] = ib_df[['SNTC SSPT Offer Flag','Contract Type','SSSNT','Uplift sspt']].apply(lambda x: (x[2] if pd.notna(x[2]) else 0) if x[0] in ['SP Base'] and x[1] in ['SPAR1'] else x[3], axis=1)
        ib_df['Uplift sspt'] = ib_df[['SNTC SSPT Offer Flag','Contract Type','SSSNE','SSSNT','Uplift sspt']].apply(lambda x: (x[2] if pd.notna(x[2]) else x[3]) if x[0] in ['SP Base'] and x[1] in ['SPAR2'] else x[4], axis=1)
        ib_df['Uplift sspt'] = ib_df[['SNTC SSPT Offer Flag','Contract Type','SSSNP','SSSNT','Uplift sspt']].apply(lambda x: (x[2] if pd.notna(x[2]) else x[3]) if x[0] in ['SP Base'] and x[1] in ['SPAR3'] else x[4], axis=1)
        ib_df['Uplift sspt'] = ib_df[['SNTC SSPT Offer Flag','Contract Type','SSS2P','SSSNT','Uplift sspt']].apply(lambda x: (x[2] if pd.notna(x[2]) else x[3]) if x[0] in ['SP Base'] and x[1] in ['SPAR4'] else x[4], axis=1)
        ib_df['Uplift sspt'] = ib_df[['SNTC SSPT Offer Flag','Contract Type','SSC2P','SSSNT','Uplift sspt']].apply(lambda x: (x[2] if pd.notna(x[2]) else x[3]) if x[0] in ['SP Base'] and x[1] in ['SPC2P'] else x[4], axis=1)
        ib_df['Uplift sspt'] = ib_df[['SNTC SSPT Offer Flag','Contract Type','SSC4P','Uplift sspt']].apply(lambda x: (x[2] if pd.notna(x[2]) else 0) if x[0] in ['SP Base'] and x[1] in ['SPC4P'] else x[3], axis=1)
        ib_df['Uplift sspt'] = ib_df[['SNTC SSPT Offer Flag','Contract Type','SSSNT','Uplift sspt']].apply(lambda x: (x[2] if pd.notna(x[2]) else 0) if x[0] in ['SP Base'] and x[1] in ['SBAR1'] else x[3], axis=1)
        ib_df['Uplift sspt'] = ib_df[['SNTC SSPT Offer Flag','Contract Type','SSCS','SSSNT','Uplift sspt']].apply(lambda x: (x[2] if pd.notna(x[2]) else x[3]) if x[0] in ['SP Base'] and x[1] in ['SPCS'] else x[4], axis=1)
        ib_df['Uplift sspt'] = ib_df[['SNTC SSPT Offer Flag','Contract Type','SSSNT','Uplift sspt']].apply(lambda x: (x[2] if pd.notna(x[2]) else 0) if x[0] in ['TELEPRESENCE CUSTOMERS'] and x[1] in ['ECDN'] else x[3], axis=1)
        ib_df['Uplift sspt'] = ib_df[['SNTC SSPT Offer Flag','Contract Type','SSCS','Uplift sspt']].apply(lambda x: (x[2] if pd.notna(x[2]) else 0) if x[0] in ['TELEPRESENCE CUSTOMERS'] and x[1] in ['ECDO'] else x[3], axis=1)
        ib_df['Uplift sspt'] = ib_df[['Contract Type','SSSNC','Uplift sspt']].apply(lambda x: (x[1] if pd.notna(x[1]) else 0) if x[0] == 'SNC' else x[2], axis=1)
        ib_df['Uplift sspt'] = ib_df[['Contract Type','SSNCO','SSSNT','Uplift sspt']].apply(lambda x: (x[1] if pd.notna(x[1]) else x[2]) if x[0] == 'SNCO' else x[3], axis=1)
        ib_df['Uplift sspt'] = ib_df[['Contract Type','SSSNP','SSSNT','Uplift sspt']].apply(lambda x: (x[1] if pd.notna(x[1]) else x[2]) if x[0] == 'PSUP' else x[3], axis=1)
        ib_df['Uplift sspt'] = ib_df[['Contract Type','SSSNT','Uplift sspt']].apply(lambda x: (x[1] if pd.notna(x[1]) else 0) if x[0] == 'PSRT' else x[2], axis=1)
        ib_df['Uplift sspt'] = ib_df[['Contract Type','SSSNT','Uplift sspt']].apply(lambda x: (x[1] if pd.notna(x[1]) else 0) if x[0] == 'PSUT' else x[2], axis=1)
        ib_df['Uplift sspt'] = ib_df[['Contract Type','SSSNT','Uplift sspt']].apply(lambda x: (x[1] if pd.notna(x[1]) else 0) if x[0] == 'LSNT' else x[2], axis=1)

        ib_df['Uplift sspt'] = ib_df['Uplift sspt'].fillna(0)
        ib_df.drop(columns=['cs multiplier sl','CS SSPT (Multiplier)'], axis=1, inplace=True)
        #(ib_df['Uplift sspt'] * ib_df['Item Quantity']).sum()
        return ib_df
    else: 
        ib_df['Uplift sspt'] = 0
        return ib_df

def upload_data_to_sf(df,user, table_name, name,cnn):

    # try:
    #     cnn = snowflake.connector.connect(
    #         user=user,
    #         authenticator='externalbrowser',
    #         role='CX_CA_BUS_ANALYST_ROLE',
    #         warehouse='CX_CA_RPT_WH',
    #         database='CX_DB',
    #         schema='CX_CA_BR',
    #         account='cisco.us-east-1'
    #     )
    #     cs = cnn.cursor()
    # except: 
    #     print(bcolors.FAIL + bcolors.BOLD + 'Error connecting to Snowflake: ' + bcolors.ENDC + "Check your credentials and VPN connection\n")
    #     raise
    
    if cnn.is_closed():
        cs,cnn=connec_to_sf(user)
    
    print(f'Uploading {name}...')
    
    if len(df) > 0:
        write_pandas(
            conn=cnn,
            df=df,
            table_name = table_name
            #table_name='EMEA_COV_COMPASS_SN_LOG'
        )
        print(f'{name} Dataframe sucessfully uploaded\n')

    else:
        print(f'Alert!, Empty {name} Dataframe\n')

def get_smartsheet_IDs(user):
    import datetime
    import smartsheet_lib as smartsheet, json

    auth_path = r".\credentials.json"
    auth_path = auth_path.replace("\\","/")
    with open(auth_path) as json_file:
        json_credentials = json.load(json_file)
        json_file.close()

    user = json_credentials["cisco_tableau"]["user"] # Your Cisco e-mail address

    date = datetime.datetime.today()
    date = date.date()
    if date.weekday() in [0,1]:
        delta_t = str(date-datetime.timedelta(4))
    else:
        delta_t = str(date-datetime.timedelta(1))

    smartsheet_client = smartsheet.init_conn(json_credentials["smartsheet"]["API_access_token"])
    #oa_sheet = smartsheet.load_sheet(7190965472520068,client=smartsheet_client,modified_since=delta_t)  #Q3
    #oa_sheet = smartsheet.load_sheet(1763453217073028,client=smartsheet_client,modified_since=delta_t)  #Q4
    #oa_sheet = smartsheet.load_sheet(3815245262153604,client=smartsheet_client,modified_since=delta_t)  #Q4-2
    oa_sheet = smartsheet.load_sheet(7298470953412484,client=smartsheet_client,modified_since=delta_t) # Q1FY23
    oa_df = pd.DataFrame()
    sheet_new = smartsheet.get_last_n_rows(oa_sheet,n_rows=4000)
    oa_df = smartsheet.sheet_to_df2(sheet_new,columns=oa_sheet.columns)
    oa_df = oa_df.query("`Request ID` != ''")
    oa_df['Request ID'] = oa_df['Request ID'].apply(lambda x:int(x)) # drop request id decimal places

    fields_smartsheet = get_da_requests(da=user,df=oa_df)
    smartsheet_IDs=",".join(map(str,fields_smartsheet["Request ID"]))
    return smartsheet_IDs, fields_smartsheet


def get_OE_pending_requests(user,cs):
    """
    Get OE pending accounts data from Snowflake with creation date mayor to '2021-08-01', 
    OP-URL Null, STATUS IN ('Validated', 'QA Approved', 'Review required') AND CAMPAIGN NOT IN ('SAVM'),
    and creates a DataFrame

    Param: user - cisco e-mail address
    """
    smartsheet_IDs, fields_smartsheet=get_smartsheet_IDs(user)
    
    if smartsheet_IDs=="":
        return print("\nNo accounts found")

    else:
        #cs=connec_to_sf(user)
        if cs.is_closed():
            cs,cnn=connec_to_sf(user)
        
        pd.options.display.max_columns = 65
        
        # # query_OE_account_list = f"""SELECT  '' AS OE_KEY,
        #                                     REQUEST_ID,
        #                                     REQUESTED_DTM AS DATE_CREATED,
        #                                     '' AS ASSIGNED_DA,
        #                                     CAMPAIGN_NAME,
        #                                     ACCOUNT_NAME AS CUSTOMER_NAME,
        #                                     '' AS INPUT_FILE_URL,
        #                                     CUSTOMER_IDENTIFIER AS ID_TYPE,
        #                                     SAV_ID,
        #                                     CAV_ID,
        #                                     '' AS CAV_BU_ID,
        #                                     GU_ID,
        #                                     SALES_LEVEL1 AS LVL1,
        #                                     SALES_LEVEL2 AS LVL2,
        #                                     CONTRACT_NUMBER AS CONTRACT_ID,
        #                                     INVENTORY_NAME,
        #                                     APPLIANCE_ID,
        #                                     CR_PARTY_NAME,
        #                                     CR_PARTY_ID,
        #                                     REQUESTER_COMMENTS AS COMMENTS,
        #                                     '' AS DA_COMMENTS,
        #                                     '' AS "STATUS",
        #                                     REQUESTER_NAME,
        #                                     '' AS POC_AFTER_COMPLETION_OF_ANALYSIS,
        #                                     "STATUS" AS OP_STATUS,
        #                                     '' AS DA_ASSIGNED_DATE,
        #                                     '' AS OP_COMPLETE_DATE,
        #                                     '' AS UPLOADED_TIME,
        #                                     '' AS OA_PACKAGE_TYPE,
        #                                     OP_URL AS OA_URL,
        #                                     '' AS ACCOUNT_VALIDATION,
        #                                     '' AS DUPLICATED_ACCOUNT,
        #                                     '' AS SNTC_CTU_OPPTY_VALUE,
        #                                     '' AS ACCOUNT_AGE,
        #                                     '' AS COMPASS_TYPE,
        #                                     CURRENT_DATE() AS ARRIVAL_DATE,
        #                                     CURRENT_TIMESTAMP() AS ARRIVAL_DATE_HRS,
        #                                     MODIFIED_DTM AS MODIFIED,
        #                                     '' AS VALIDATION,
        #                                     '' AS REJECTED_REASON,
        #                                     '' AS SUB_STATUS,
         #                                   '' AS STAGE
        #                         FROM CX_DB.CX_CA_BR.REQUEST_TRACKER_DETAILS
        #                         WHERE REQUESTED_DTM >= '2021-08-01' AND OP_URL IS NULL AND STATUS IN ('Validated','ETL Completed', 'In Progress',  'Review Required (Ops)' , 'Review Required (Dev)') AND CAMPAIGN NOT IN ('SAVM')
        #                         """

        query_OE_account_list = f"""SELECT  '' AS OE_KEY,
                                            REQUEST_ID,
                                            REQUESTED_DTM AS DATE_CREATED,
                                            '' AS ASSIGNED_DA,
                                            CAMPAIGN_NAME,
                                            ACCOUNT_NAME AS CUSTOMER_NAME,
                                            '' AS INPUT_FILE_URL,
                                            CUSTOMER_IDENTIFIER AS ID_TYPE,
                                            SAV_ID,
                                            CAV_ID,
                                            '' AS CAV_BU_ID,
                                            GU_ID,
                                            SALES_LEVEL1 AS LVL1,
                                            SALES_LEVEL2 AS LVL2,
                                            CONTRACT_NUMBER AS CONTRACT_ID,
                                            INVENTORY_NAME,
                                            APPLIANCE_ID,
                                            CR_PARTY_NAME,
                                            CR_PARTY_ID,
                                            REQUESTER_COMMENTS AS COMMENTS,
                                            '' AS DA_COMMENTS,
                                            '' AS "STATUS",
                                            REQUESTER_NAME,
                                            '' AS POC_AFTER_COMPLETION_OF_ANALYSIS,
                                            "STATUS" AS OP_STATUS,
                                            '' AS DA_ASSIGNED_DATE,
                                            '' AS OP_COMPLETE_DATE,
                                            '' AS UPLOADED_TIME,
                                            '' AS OA_PACKAGE_TYPE,
                                            OP_URL AS OA_URL,
                                            '' AS ACCOUNT_VALIDATION,
                                            '' AS DUPLICATED_ACCOUNT,
                                            '' AS SNTC_CTU_OPPTY_VALUE,
                                            '' AS ACCOUNT_AGE,
                                            '' AS COMPASS_TYPE,
                                            CURRENT_DATE() AS ARRIVAL_DATE,
                                            CURRENT_TIMESTAMP() AS ARRIVAL_DATE_HRS,
                                            MODIFIED_DTM AS MODIFIED,
                                            '' AS VALIDATION,
                                            '' AS REJECTED_REASON,
                                            '' AS SUB_STATUS,
                                            '' AS STAGE
                                FROM CX_DB.CX_CA_BR.REQUEST_TRACKER_DETAILS
                                WHERE REQUEST_ID IN ({smartsheet_IDs})
                                """

        cir_columns = ['OE_KEY','REQUEST_ID','DATE_CREATED','ASSIGNED_DA','CAMPAIGN_NAME','CUSTOMER_NAME','INPUT_FILE_URL',
                        'ID_TYPE','SAV_ID','CAV_ID','CAV_BU_ID','GU_ID','LVL1','LVL2','CONTRACT_ID','INVENTORY_NAME','APPLIANCE_ID',
                        'CR_PARTY_NAME','CR_PARTY_ID','COMMENTS','DA_COMMENTS','STATUS','REQUESTER_NAME','POC_AFTER_COMPLETION_OF_ANALYSIS',
                        'OP_STATUS','DA_ASSIGNED_DATE','OP_COMPLETE_DATE','UPLOADED_TIME','OA_PACKAGE_TYPE','OA_URL','ACCOUNT_VALIDATION',
                        'DUPLICATED_ACCOUNT','SNTC_CTU_OPPTY_VALUE','ACCOUNT_AGE','COMPASS_TYPE','ARRIVAL_DATE','ARRIVAL_DATE_HRS','MODIFIED',
                        'VALIDATION','REJECTED_REASON','SUB_STATUS','STAGE']
        
        print("\nRetrieving accounts with pending OE reports from the COMPASS REQUEST TRACKER (Snowlake)\n")
        cs.execute(query_OE_account_list)

        fields_df = pd.DataFrame(cs.fetchall(), columns=cir_columns)
        
        if len(fields_df) > 0:
            
            print(f"\n{len(fields_df)} accounts found:")
            
            fields_df['OE_KEY'] = fields_df['REQUEST_ID'].astype(int) * 4 
            fields_df['DATE_CREATED'] = fields_df['DATE_CREATED'].dt.date
            fields_df['DA_ASSIGNED_DATE'] = fields_df['DA_ASSIGNED_DATE'].replace('',pd.NA)
            fields_df['OP_COMPLETE_DATE'] = fields_df['OP_COMPLETE_DATE'].replace('',pd.NA)
            fields_df['UPLOADED_TIME'] = fields_df['UPLOADED_TIME'].replace('',pd.NA)

            pd.options.display.max_columns=30
            fields_df["SAV_ID"]=fields_df["SAV_ID"].fillna("0")
            
            fields_smartsheet=fields_smartsheet[["Request ID"]]
            fields_df=fields_smartsheet.merge(fields_df, left_on="Request ID",right_on="REQUEST_ID", how="left").drop(columns=["Request ID"],axis=1)
            fields_df["REQUEST_ID"]=fields_df["REQUEST_ID"].astype(int)
            fields_df['INVENTORY_NAME'] = fields_df['INVENTORY_NAME'].apply(lambda x: None if x is  None else str(x)[:499])
            fields_df['CONTRACT_ID'] = fields_df['CONTRACT_ID'].apply(lambda x: None if x is  None else str(x)[:499])
            fields_df['APPLIANCE_ID'] = fields_df['APPLIANCE_ID'].apply(lambda x: None if x is  None else str(x)[:499])
            fields_df['CR_PARTY_NAME'] = fields_df['CR_PARTY_NAME'].apply(lambda x: None if x is  None else str(x)[:499])
            fields_df['CR_PARTY_ID'] = fields_df['CR_PARTY_ID'].apply(lambda x: None if x is  None else str(x)[:499])
            return fields_df
            
        else:
            print("No pending accounts found")
            return fields_df
        
        
def adapt_OE_data(renew_filtered, coverage_filtered, tac_filtered, telemetry_filtered, dna_filtered, EA_details_filtered, EA_eligibility_filtered):
    """
    Sort and select especific columns for the passed dataframes. 
    Returns a dataframe ready to be uploaded to Snowflake. 

    Param: 
        Dataframes of: renew, coverage, tac, telemetry, dna, EA details and EA eligibility.
    """
    import datetime

    # ------------- IB

    renew_filtered = renew_filtered[['OE_KEY','REQUEST_ID','Coverage','Contract Number','Covered Line Status','Contract Type','Contract Line End Quarter','Contract Line End Fiscal Year','Instance Shipped Fiscal Year','Offer Type Name','Asset Type','LDoS','Business Entity Name','Sub Business Entity Name','Product Family','Product ID','Product Type','Default Service List Price USD','Item Quantity','Annual Extended Contract Line List USD Amount','Annual Contract Line Net USD Amount','Annualized Extended Contract Line List USD Amount','Annualized Contract Line Net USD Amount','Contract Line List Price USD','Contract Line Net Price USD','Asset List Amount','Convertsto','SNTC SSPT Offer Flag','Current SSPT Flag','Offer Category L1(CX FAST)','Service Level','Multiplier','Uplift','ADJUSTED_CATEGORY','sntc_pricing','sspt_pricing','success_tracks_pricing_L1','success_tracks_pricing_L2','Eligible','CHANNEL_PARTNER_NAME','Service Brand Code','SUCCESS_TRACK_YORN','Best Site Sales Level 2 Name']]
    renew_filtered.columns = ['OE_KEY','REQUEST_ID','COVERAGE','CONTRACT_NUMBER','COVERED_LINE_STATUS','CONTRACT_TYPE','CONTRACT_LINE_END_QUARTER','CONTRACT_LINE_END_FISCAL_YEAR','INSTANCE_SHIPPED_FISCAL_YEAR','OFFER_TYPE_NAME','ASSET_TYPE','LDOS','BUSINESS_ENTITY_NAME','SUB_BUSINESS_ENTITY_NAME','PRODUCT_FAMILY','PRODUCT_ID','PRODUCT_TYPE','DEFAULT_SERVICE_LIST_PRICE_USD','ITEM_QUANTITY','ANNUAL_EXTENDED_CONTRACT_LINE_LIST_USD_AMOUNT','ANNUAL_CONTRACT_LINE_NET_USD_AMOUNT','ANNUALIZED_EXTENDED_CONTRACT_LINE_LIST_USD_AMOUNT','ANNUALIZED_CONTRACT_LINE_NET_USD_AMOUNT','CONTRACT_LINE_LIST_PRICE_USD','CONTRACT_LINE_NET_PRICE_USD','ASSET_LIST_AMOUNT','CONVERTSTO','SNTC_SSPT_OFFER_FLAG','CURRENT_SSPT_FLAG','OFFER_CATEGORY_L1','SERVICE_LEVEL','MULTIPLIER','UPLIFT','ADJUSTED_CATEGORY','SNTC_PRICING','SSPT_PRICING','SUCCESS_TRACKS_PRICING_L1','SUCCESS_TRACKS_PRICING_L2','ELIGIBLE','CHANNEL_PARTNER_NAME','SERVICE_BRAND_CODE','SUCCESS_TRACK_YORN','SALES_LEVEL_2']
    renew_filtered["LDOS"]=renew_filtered["LDOS"].fillna(datetime.date(3500,1,1)).astype(str) #Fill empty dates and change dtype to (str)

    # ------------- Coverage
    coverage_filtered = coverage_filtered[['OE_KEY','REQUEST_ID','Coverage','Asset List Amount','Annual Contract Line Net USD Amount','Contract Line Net Price USD']]
    coverage_filtered.columns = ['OE_KEY','REQUEST_ID','COVERAGE','ASSET_LIST_AMOUNT','ANNUAL_CONTRACT_LINE_NET_USD_AMOUNT','CONTRACT_LINE_NET_PRICE_USD']

    # -------------- TAC
    tac_filtered = tac_filtered[['OE_KEY','REQUEST_ID','MAX_SEVERITY_INT','SR Creation FY Quarter','INCDT_CLOSED_FISCAL_QTR_NM','CURRENT_SERIAL_NUMBER','BUG_CNT','CONTRACT NUMBER','CONTRACT_TYPE','RESOLUTION_CODE','PART_NUMBER','HYBRID_PRODUCT_FAMILY','SUB_TECH_NAME','SR_PRODUCT_ID','CASE_NUMBER','PARTY ID','PARTY NAME','ID','BE_INT','Data Extracted Date','ADJUSTED_CATEGORY','Initial Time to Resolution','Final Time to Resolution','Customer Ownership Time','Delivery Ownership Time','INCIDENT_NUMBER','Product SKU','INCDT_CREATION_DATE']]
    tac_filtered.columns = ['OE_KEY','REQUEST_ID','MAX_SEVERITY_INT','SR_CREATION_FY_QUARTER','INCDT_CLOSED_FISCAL_QTR_NM','CURRENT_SERIAL_NUMBER','BUG_CNT','CONTRACT_NUMBER','CONTRACT_TYPE','RESOLUTION_CODE','PART_NUMBER','HYBRID_PRODUCT_FAMILY','SUB_TECH_NAME','SR_PRODUCT_ID','CASE_NUMBER','PARTY_ID','PARTY_NAME','ID','BE_INT','DATA_EXTRACTED_DATE','ADJUSTED_CATEGORY','INITIAL_TIME_TO_RESOLUTION','FINAL_TIME_TO_RESOLUTIOM','CUSTOMER_OWNERSHIP_TIME','DELIVERY_OWNERSHIP_TIME','INCIDENT_NUMBER','ST_PRODUCT_SKU','CREATION_DATE']
    tac_filtered["DATA_EXTRACTED_DATE"]=tac_filtered["DATA_EXTRACTED_DATE"].fillna(datetime.date(3500,1,1)).astype(str) #Fill empty dates and change dtype to (str)
    tac_filtered['CREATION_DATE'] = tac_filtered['CREATION_DATE'].astype('str')
    tac_filtered["CREATION_DATE"]=tac_filtered["CREATION_DATE"].fillna(datetime.date(3500,1,1)).astype(str)

    # -------------- Telemetry
    telemetry_filtered = telemetry_filtered[['OE_KEY','REQUEST_ID','Party ID','ACTIVE_YORN','Equipment Type Description','Appliance ID','Inventory','Collection Date','Product ID','Product Family','Business Entity','Sub Business Entity','Equipment Type','Product Type','Last Date of Support','Contract Number','Contract Lines Status','Updated Date','SSPT_YORN','ADJUSTED_CATEGORY','Serial Number','SNT']]
    telemetry_filtered.columns = ['OE_KEY','REQUEST_ID','PARTY_ID','ACTIVE_YORN','EQUIPMENT_TYPE_DESCRIPTION','APPLIANCE_ID','INVENTORY','COLLECTION_DATE','PRODUCT_ID','PRODUCT_FAMILY','BUSINESS_ENTITY','SUB_BUSINESS_ENTITY','EQUIPMENT_TYPE','PRODUCT_TYPE','LAST_DATE_OF_SUPPORT','CONTRACT_NUMBER','CONTRACT_LINES_STATUS','UPDATED_DATE','ELIGIBLE','ADJUSTED_CATEGORY','SERIAL_NUMBER','SNT']
    telemetry_filtered["LAST_DATE_OF_SUPPORT"]=pd.to_datetime(telemetry_filtered["LAST_DATE_OF_SUPPORT"]).astype(str).replace("NaT","3500-01-01")
    telemetry_filtered[["COLLECTION_DATE","UPDATED_DATE"]]=telemetry_filtered[["COLLECTION_DATE","UPDATED_DATE"]].fillna(datetime.date(3500,1,1)).astype(str) #Fill empty dates and change dtype to (str)

    # -------------- DNA
    dna_filtered = dna_filtered[['OE_KEY','REQUEST_ID','CX_USECASE_NAME','MAX_LIFECYCLE_STAGE_NAME','CUSTOMER_MAX_LIFE_CYCLE_STAGE']]
    dna_filtered.columns = ['OE_KEY','REQUEST_ID','CX_USECASE_NAME','MAX_LIFECYCLE_STAGE_NAME','CUSTOMER_MAX_LIFE_CYCLE_STAGE']

    # -------------- EA
    EA_details_filtered=EA_details_filtered[['OE_KEY','REQUEST_ID','ACCOUNT_ID','ACCOUNT_NAME','ARCHITECTURE','SUB_ARCHITECTURE','PRODUCT_ID','PRODUCT_FAMILY','PRODUCT_CATEGORY','SSPT_LIST_PRICE','ITEM_QTY','LIST_PRICE','ACCOUNT_IDENTIFIER']]
    EA_details_filtered.columns = ['OE_KEY','REQUEST_ID','ACCOUNT_ID','ACCOUNT_NAME','ARCHITECTURE','SUB_ARCHITECTURE','PRODUCT_ID','PRODUCT_FAMILY','PRODUCT_CATEGORY','SSPT_LIST_PRICE','ITEM_QTY','LIST_PRICE','ACCOUNT_IDENTIFIER']

    EA_eligibility_filtered=EA_eligibility_filtered[['OE_KEY','REQUEST_ID','ACCOUNT_ID','ACCOUNT_NAME','ARCHITECTURE','EXISTING_EA_CUST_ID_FLAG','EXISTING_EA_CUST_NAME_FLAG','EA_MIG_ELIG_CUST_ID_FLAG','EA_MIG_ELIG_CUST_NAME_FLAG',"SSPT_LIST_PRICE","ITEM_QTY",'ACCOUNT_IDENTIFIER']]
    EA_eligibility_filtered.columns = ['OE_KEY','REQUEST_ID','ACCOUNT_ID','ACCOUNT_NAME','ARCHITECTURE','EXISTING_EA_CUST_ID_FLAG','EXISTING_EA_CUST_NAME_FLAG','EA_MIG_ELIG_CUST_ID_FLAG','EA_MIG_ELIG_CUST_NAME_FLAG',"SSPT_LIST_PRICE","ITEM_QTY",'ACCOUNT_IDENTIFIER']

    return renew_filtered, coverage_filtered, tac_filtered, telemetry_filtered, dna_filtered, EA_details_filtered, EA_eligibility_filtered


def oe_data_cleaning(renew_filtered, coverage_filtered, tac_filtered,telemetry_filtered, dna_filtered, EA_details_filtered, EA_eligibility_filtered):
    
    # ---------------------------------------- IB data
    renew_filtered = fill_nas(renew_filtered)
    renew_filtered[['LDoS FY','Instance Shipped Fiscal Year','Contract Number']] = renew_filtered[['LDoS FY','Instance Shipped Fiscal Year','Contract Number']].fillna(0).apply(pd.to_numeric, downcast='integer')
    renew_filtered['Item Quantity'] = renew_filtered['Item Quantity'].astype(float)
    renew_filtered["sspt_pricing"]=renew_filtered["sspt_pricing"].astype(float).apply(lambda x: round(x, 2))

    # ---------------------------------------- Coverage
    coverage_filtered = fill_nas(coverage_filtered)
    coverage_filtered['CUSTOMER_ID'] = coverage_filtered['CUSTOMER_ID'].astype(str)
    coverage_filtered['Item Quantity'] = coverage_filtered['Item Quantity'].astype(int)

    # ---------------------------------------- TAC
    tac_filtered['ID'] = tac_filtered['ID'].astype(str)
     
    # ---------------------------------------- Telemetry
    telemetry_filtered['ID'] = telemetry_filtered['ID'].astype(str)
    telemetry_filtered['ACTIVE_YORN'] = 'Y'
    parties_active_collectors = telemetry_filtered["Party ID"].unique().astype(str).tolist()
    parties_active_collectors = ",".join(parties_active_collectors)

    # ---------------------------------------- DNA
    dna_filtered['CUSTOMER_ID'] = dna_filtered['CUSTOMER_ID'].astype(str)

    # ---------------------------------------- EA
    EA_details_filtered = fill_nas(EA_details_filtered)
    EA_details_filtered["ACCOUNT_ID"]=EA_details_filtered["ACCOUNT_ID"].astype(str)

    EA_eligibility_filtered = fill_nas(EA_eligibility_filtered)
    EA_eligibility_filtered["ACCOUNT_ID"]=EA_eligibility_filtered["ACCOUNT_ID"].astype(str)

    return renew_filtered, coverage_filtered, tac_filtered,telemetry_filtered, dna_filtered, EA_details_filtered, EA_eligibility_filtered


def split_account_ids(fields_df):
    savs = fields_df.query("`ID_TYPE` == 'SAV ID'")[["REQUEST_ID",'OE_KEY',"SAV_ID"]]
    savs = savs.rename(columns={"SAV_ID": 'ID'}).astype(str)
    gus = fields_df.query("`ID_TYPE` == 'GU ID'")[["REQUEST_ID",'OE_KEY',"GU_ID"]]
    gus = gus.rename(columns={"GU_ID": 'ID'}).astype(str)
    cavs = fields_df.query("`ID_TYPE` == 'CAV ID'")[["REQUEST_ID",'OE_KEY',"CAV_ID"]]
    cavs = cavs.rename(columns={"CAV_ID": 'ID'}).astype(str)
    crs = fields_df.query("`ID_TYPE` == 'CR Party ID'")[["REQUEST_ID",'OE_KEY',"CR_PARTY_ID"]]
    crs = crs.rename(columns={"CR_PARTY_ID": 'ID'}).astype(str)

    request = pd.concat([savs,gus,cavs,crs], axis=0).reset_index(drop=True)
    request['REQUEST_ID'] = request['REQUEST_ID'].astype(int)
    #request['OE_KEY'] = request['REQUEST_ID'] * 4

    row_drops = []
    for i, row in request.iterrows():
        if ',' in row['ID']:
            for j in row['ID'].split(','):
                new = [row['REQUEST_ID'],row['OE_KEY'],j]
                request.loc[len(request)] = new
            print('Request ID {} modified'.format(row['REQUEST_ID']))
            row_drops.append(i)

    request.drop(row_drops, inplace=True)

    return savs, gus, cavs, crs, request


def add_OE_KEY(renew_filtered, coverage_filtered, tac_filtered, telemetry_filtered, dna_filtered, EA_details_filtered, EA_eligibility_filtered, request):

    ## -------- IB
    renew_filtered = renew_filtered.merge(request, how='left', left_on='customer_id', right_on='ID')
    renew_filtered.drop(columns=['ID'], axis=1, inplace=True)
    renew_filtered = renew_filtered.sort_values('REQUEST_ID')

    ## -------- Coverage,
    coverage_filtered = coverage_filtered.merge(request, how='left', left_on='CUSTOMER_ID', right_on='ID')
    coverage_filtered.drop(columns=['ID'], axis=1, inplace=True)
    coverage_filtered = coverage_filtered.sort_values('REQUEST_ID')

    ## --------- TAC
    tac_filtered = tac_filtered.merge(request, how='left', left_on='ID', right_on='ID')
    tac_filtered = tac_filtered.sort_values('REQUEST_ID')

    ## --------- Telemetry
    telemetry_filtered = telemetry_filtered.merge(request, how='left', left_on='ID', right_on='ID')
    telemetry_filtered = telemetry_filtered.sort_values('REQUEST_ID')

    ## --------- DNA
    dna_filtered = dna_filtered.merge(request, how='left', left_on='CUSTOMER_ID', right_on='ID')
    dna_filtered.drop(columns=['ID'], axis=1, inplace=True)
    dna_filtered = dna_filtered.sort_values('REQUEST_ID')

    ## --------- EA
    EA_details_filtered = EA_details_filtered.merge(request, how='left', left_on='ACCOUNT_ID', right_on='ID')
    EA_details_filtered.drop(columns=['ID'], axis=1, inplace=True)

    EA_eligibility_filtered = EA_eligibility_filtered.merge(request, how='left', left_on='ACCOUNT_ID', right_on='ID')
    EA_eligibility_filtered.drop(columns=['ID'], axis=1, inplace=True)

    return renew_filtered, coverage_filtered, tac_filtered, telemetry_filtered, dna_filtered, EA_details_filtered, EA_eligibility_filtered



def update_OE_tracker(user, fields_df, renew_filtered, telemetry_filtered):
    import datetime
    op_tracker = fields_df.copy()

    op_tracker['OA_URL'] = op_tracker['OE_KEY'].apply(lambda x: f"https://cx-tableau-stage.cisco.com/t/Compass/views/Offer_Estimator/EstimatorRecommendationsSummary?%3Aiid=5&RUN_ID={x}&iframeSizedToWindow&%3Aembed=y&%3AshowAppBanner=false&%3Adisplay_count=no&%3AshowVizHome=no&%3Atabs=no&%3Aorigin=viz_share_link&%3Atoolbar=yes#3")
    print("1) URLs created")

    #Assigned DA
    op_tracker["ASSIGNED_DA"]=user
    print(f"2) Accounts assigned to DA: {user}")

    #Report Type
    len_renew_filtered=renew_filtered.groupby(["REQUEST_ID"]).count()[["OE_KEY"]].reset_index().rename(columns={"OE_KEY":"renew_filtered"})
    len_telemetry_filtered=telemetry_filtered.groupby(["REQUEST_ID"]).count()[["OE_KEY"]].reset_index().rename(columns={"OE_KEY":"telemetry_filtered"})
    compass_report_type=pd.merge(len_renew_filtered,len_telemetry_filtered, how="left", on=["REQUEST_ID","REQUEST_ID"]).fillna(0)
    compass_report_type["REQUEST_ID"]=compass_report_type["REQUEST_ID"].astype(str)

    def report_type(renew_filtered,telemetry_filtered):
        if renew_filtered > 0:
            if telemetry_filtered == 0:
                return("Lite")
            else:
                return("Prime")
        else:
                return("No Report")

    compass_report_type["COMPASS_REPORT_TYPE"]=compass_report_type.apply(lambda x: report_type(x.renew_filtered,x.telemetry_filtered),axis=1)
    #compass_report_type["REQUEST_ID"]=compass_report_type["REQUEST_ID"].astype(int)
    
    op_tracker = op_tracker.astype({'REQUEST_ID':str})
    op_tracker=op_tracker.merge(compass_report_type[["REQUEST_ID","COMPASS_REPORT_TYPE"]], how="left", left_on="REQUEST_ID", right_on="REQUEST_ID")
    op_tracker["OA_PACKAGE_TYPE"]=op_tracker["COMPASS_REPORT_TYPE"]
    op_tracker["OA_PACKAGE_TYPE"]=op_tracker["OA_PACKAGE_TYPE"].fillna('No Report')
    op_tracker["OA_URL"]=op_tracker[["OA_PACKAGE_TYPE","OA_URL"]].apply(lambda x: 'N/A' if x[0] == 'No Report' else x[1], axis = 1)
    
    op_tracker.drop(columns="COMPASS_REPORT_TYPE", inplace=True)
    
    #Rejected reason:
    op_tracker["REJECTED_REASON"]=op_tracker["STATUS"].apply(lambda x: "No IB data" if x=="N/A" else "")
    print("3) Entered rejected reason for accounts with no IB data")

    #OP Status:
    #op_tracker["STATUS"]="Closed"
    #print("4) Status of accounts set as 'Closed'")

    #OP Complete date:
    #op_tracker[["DA_ASSIGNED_DATE","OP_COMPLETE_DATE","UPLOADED_TIME"]]=datetime.date.today() #Fillna and default date "3500,1,1" line remove as well
    op_tracker[["DA_ASSIGNED_DATE","OP_COMPLETE_DATE"]]=datetime.date.today()
    
    #Change to (str) to upload to SF without issues
    op_tracker[["DATE_CREATED","DA_ASSIGNED_DATE","OP_COMPLETE_DATE","ARRIVAL_DATE"]]=op_tracker[["DATE_CREATED","DA_ASSIGNED_DATE","OP_COMPLETE_DATE","ARRIVAL_DATE"]].fillna(datetime.date(3500,1,1)).astype(str)

    print(f"5) Complete date set as: {datetime.date.today()}")

    return op_tracker

def update_compass_request_tracker(op_tracker):
    
    import compass_lib as compass
    
    # Connection to Compass API:
    access_token,configuration,user_info = compass.init_conn()
    not_uploaded = []
    i=1

    #Lopp to upload each account URL and REPORT TYPE:    
    for idx in op_tracker.index:
        
        #Generation of variables:
        req_id = op_tracker['REQUEST_ID'][idx]
        oe_url = op_tracker['OA_URL'][idx]
        oe_type = op_tracker['OA_PACKAGE_TYPE'][idx]
        #if op_tracker['OA_PACKAGE_TYPE'][idx] == "COMPASS PRIME":
        #    oe_type = "Prime"
        #elif op_tracker['OA_PACKAGE_TYPE'][idx] == "COMPASS LITE":
        #    oe_type = "Lite"
        #elif op_tracker['OA_PACKAGE_TYPE'][idx] == "N/A":
        #    oe_type = "No Report"

        print(f"{i}/{len(op_tracker)} - {req_id}")
        body = {"opUrl": oe_url,
                "oaPackageType": oe_type}
        print(f"{body}\n")
        
        #Upload of the URL and REPORT TYPE:
        try:
            patch_response = compass.patch_request(configuration=configuration, request_id=req_id, body=body)       
            if patch_response is None:
                raise AssertionError()
            else:
                op_tracker["UPLOADED_TIME"][idx]=str(datetime.date.today())
                print('Data uploaded!')
        except:
            print(bcolors.FAIL + 'Compass Tracker - Request ID not updated:' + str(op_tracker['REQUEST_ID'][idx]) + bcolors.ENDC)
            not_uploaded.append(req_id)
            i+=1
    return op_tracker

def upload_to_compass_tracker(op_tracker):
    # Connection to Compass API
    import compass_lib as compass
    from datetime import datetime

    access_token,configuration,user_info = compass.init_conn()
    not_uploaded = []

    i=1
    
    #Loop to upload each account:
    for idx in op_tracker.index:
        req_id = op_tracker['REQUEST_ID'][idx]
        oe_url = op_tracker['OA_URL'][idx]
        oe_type = op_tracker['OA_PACKAGE_TYPE'][idx]

        print(f"{i}/{len(op_tracker)} - Request ID: {req_id}", end=" ")
        body = {"opUrl": oe_url,
                "oaPackageType": oe_type}
        #print(f"{body}\n")
        
        try:
            patch_response = compass.patch_request(configuration=configuration, request_id=req_id, body=body)       
            if patch_response is None:
                raise AssertionError()
            else:
                #If uploaded, updates the date in the op_tracker on that account:
                op_tracker["UPLOADED_TIME"][idx]=str(datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
                op_tracker["STATUS"][idx]="Closed"
                print(bcolors.OKGREEN +'UPLOADED'+ bcolors.ENDC)

        except:
            op_tracker["STATUS"][idx]="On Hold"
            op_tracker["UPLOADED_TIME"][idx]=""
            print(bcolors.FAIL + 'NOT UPLOADED:'+bcolors.ENDC)
            not_uploaded.append(req_id)
        
        i+=1
    #op_tracker=op_tracker[op_tracker["UPLOADED_TIME"].notna()]
    
    return op_tracker

def upload_data_to_backup(user, table_name, backup_table_name, name):

    cs=connec_to_sf(user)
    
    query_backup = f"""INSERT INTO CX_DB.CX_CA_BR.{backup_table_name}
                        SELECT * FROM CX_DB.CX_CA_BR.{table_name}
                        WHERE REQUEST_ID NOT IN (SELECT REQUEST_ID FROM CX_DB.CX_CA_BR.{backup_table_name})
                    """
    
    print(f'{name}: ',end="")
    #cs.execute(query_tac)
    try:
        cs.execute(query_backup)
        df = pd.DataFrame(cs.fetchall())
        print(bcolors.OKGREEN +'BACKUP DONE - '+ bcolors.ENDC, end="")
        print(f"{str(sum(df[0]))} rows inserted")
        return df
    except:
        print(bcolors.FAIL +'ERROR WHEN UPLOADING'+ bcolors.ENDC + "\n")
        raise

def print_smartsheet_data(op_tracker):
    print("------------------------------------------ OE URLs ------------------------------------------")
    for link in op_tracker["OA_URL"]:
        webbrowser.open(link)
        print (link)

    print("\n------------------------------------------ Package types ------------------------------------------")
    for PACKATE_TYPE in op_tracker["OA_PACKAGE_TYPE"]:
        print(PACKATE_TYPE)

    print("\n------------------------------------------ Uploaded Time ------------------------------------------")
    for Uploaded_time in op_tracker["UPLOADED_TIME"]:
        print(Uploaded_time)

    print("\n------------------------------------------ Status ------------------------------------------")
    for status in op_tracker["STATUS"]:
        print(status)