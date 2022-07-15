from ast import NotIn
import warnings
from matplotlib.pyplot import axis

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
from tenacity import retry_if_not_result

warnings.filterwarnings("ignore")


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

    #contracts_l = fields_df.query("`Contract ID` != ''")[
     #   "contract_list"].tolist()
    #flat_cr = list(itertools.chain(*contracts_l))
    #contract_str_list = separator.join(flat_cr)

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


def get_telemetry_df2(user, savs, gus, parties, cavs):
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
    types_list = {'SAV': savs, 'GU': gus, 'CR': parties, 'CAV': cavs}

    for type_id in types_list.keys():

        ids = types_list.get(type_id)

        if ids == '':
            pass
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
                where I1.CUSTOMERID IN  (SELECT PARTY_ID FROM "CX_DB"."CX_CA_BR"."ACCOUNT_ID_LOOKUP"  WHERE ACCOUNT_ID='{type_id}' AND ID IN ({ids}))



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
                           'Contract End Date', 'Contract Line End Date', 'ACCOUNT_ID', 'ID', 'Updated Date']

            df = pd.DataFrame(df, columns=cir_columns)

            dfs.append(df)

    cir_df = pd.concat(dfs)

    #types = uncovered_df.dtypes.to_dict()
    cir_df.insert(loc=1, column='ACTIVE_YORN', value='Y')
    #cir_df = cir_df.query("ACTIVE_YORN == 'Y'")
    cir_df['Party ID'] = cir_df['Party ID'].apply(
        lambda x: int(0 if x is None else x))
    cir_df[['Contract End Date', "Updated Date", 'Contract Line End Date', 'Last Date of Support']] = cir_df[[
        'Contract End Date', "Updated Date", 'Contract Line End Date', 'Last Date of Support']].replace({pd.NaT: None})
    cir_df['Equipment Type'] = cir_df['Equipment Type'].apply(
        lambda x: float(0.0 if x is None else str(x).split('.')[0]))

    return cir_df


def get_dna_df(user, savs, gus, parties, cavs):
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
    types_list = {'SAV': savs, 'GU': gus, 'CR': parties, 'CAV': cavs}

    for type_id in types_list.keys():

        ids = types_list.get(type_id)
        id_identifyer = type_id

        print(ids)
        if ids == '':
            print("EmptyIDs")

        elif id_identifyer == 'SAV':
            print("Entered as SAV")
            query_lifecycle = f""" WITH CX AS (
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

            cs.execute(query_lifecycle)
            df = cs.fetchall()
            dna_columns = ['CUSTOMER_ID',	'CUSTOMER_NAME',	'CX_USECASE_NAME',
                           'MAX_LIFECYCLE_STAGE_NAME', 'ACCOUNT_INDENTIFIER', 'CUSTOMER_MAX_LIFE_CYCLE_STAGE']
            df = pd.DataFrame(df, columns=dna_columns)
            dfs.append(df)
            #sav_dna_df = pd.concat(dfs)

        elif id_identifyer == 'CAV':
            print('Entered as CAV')
            query_lifecycle = f"""WITH CX AS (
                    SELECT
                        CX_CUSTOMER_BU_ID,
                        CX_CUSTOMER_BU_KEY,
                        CX_CUSTOMER_BU_NAME,
                        CX_CUSTOMER_ID,
                        CX_CUSTOMER_KEY,
                        CX_CUSTOMER_NAME
                    FROM
                        CX_DB.CX_CA_EBV.BV_CXCUST_BU_PARTY_MAPPING
                    WHERE
                        CX_CUSTOMER_ID in ({ids})
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
                        LC_STG.CX_CUSTOMER_BU_KEY,
                        CX_CUSTOMER_KEY,
                        CX.CX_CUSTOMER_BU_NAME,
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
                        11,
                        12,
                        13
                    ),
                    cav AS (
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
                    CAV A
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
                        CAV
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
                        CAV
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
            dna_columns = ['CUSTOMER_ID',	'CUSTOMER_NAME',	'CX_USECASE_NAME',
                           'MAX_LIFECYCLE_STAGE_NAME', 'ACCOUNT_INDENTIFIER', 'CUSTOMER_MAX_LIFE_CYCLE_STAGE']
            df = pd.DataFrame(df, columns=dna_columns)
            dfs.append(df)
            #cav_dna_df = pd.concat(dfs)

        elif id_identifyer == 'GU':
            print("Entered as GU")
            query_lifecycle = f""" WITH CX AS (
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
                    WHERE
                        cust1.GU_PARTY_SSOT_PARTY_ID_INT in ({ids})
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
                    GU AS (
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
                    GU A
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
                        GU
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
                        GU
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
            dna_columns = ['CUSTOMER_ID',	'CUSTOMER_NAME',	'CX_USECASE_NAME',
                           'MAX_LIFECYCLE_STAGE_NAME', 'ACCOUNT_INDENTIFIER', 'CUSTOMER_MAX_LIFE_CYCLE_STAGE']
            df = pd.DataFrame(df, columns=dna_columns)
            dfs.append(df)
            #gu_dna_df = pd.concat(dfs)

        elif id_identifyer == 'CR':  # This one is the query for CR Party
            print("Entered as CR")
            query_lifecycle = f"""WITH CX AS (
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
                WHERE
                    BRANCH_PARTY_SSOT_PARTY_ID_INT in ({ids})
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
                CR AS (
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
                CR A
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
                    CR
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
                    CR
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
            dna_columns = ['CUSTOMER_ID',	'CUSTOMER_NAME',	'CX_USECASE_NAME',
                           'MAX_LIFECYCLE_STAGE_NAME', 'ACCOUNT_INDENTIFIER', 'CUSTOMER_MAX_LIFE_CYCLE_STAGE']
            df = pd.DataFrame(df, columns=dna_columns)
            dfs.append(df)

    dna_df = pd.concat(dfs)

    return dna_df


def get_tac_df_new(user, ids, id_type):
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

    return tac_df


def get_cav_names(user, ids):
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
        TableDefinition.Column('Data Extracted Date', SqlType.date())
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
    merge['LDoS'] = merge['LDoS'].apply(
        lambda x: str(x).replace('nan', '2100-01-01 00:00:00'))
    fechas = [datetime.strptime(str(i)[:-5], '%m-%d-%Y') if len(i) <
              16 else datetime.fromisoformat(str(i)) for i in merge['LDoS']]
    merge['LDoS'] = fechas

    # Calculating LDoS flag column
    merge['LDoS Flag'] = LDoS_flag(merge['LDoS'])

    # Filtering data

    merge = merge[merge['Coverage'] == 'NOT COVERED']

    #merge=merge[(merge["Asset Type"]=="Hardware")]

    merge = merge[(merge['Product Type'] != 'APPLIANCE') & (merge['Product Type'] != 'CABLE') & (
        merge['Product Type'] != 'APPSWIND') & (merge['Product Type'] != 'POWER') & (merge['Product Type'] != 'DOC')]

    merge = merge[(merge['LDoS Flag'] == 'N')]

    #merge = merge[(merge['Business Entity Name'] != 'Other')]

    # Calculating IB Value

    merge["IB Oppty"] = merge["Item Quantity"]*merge["SNT"]/1000
    sntc_oppty = merge.iloc[:, np.r_[0, -1]].groupby(merge.columns[0]).sum()

    if len(sntc_oppty) > 0:
        return sntc_oppty['IB Oppty'].sum()
    else:
        return 'N/A'



#Function that generates the CSV files

def set_datasource(df,type,folder_path,contract_types_list,
                    success_track_pricing_list,sspt_pricing_list_eligibleSSPT,
                    sspt_pricing_list_outputTable,sntc_pricing_list,htec_contract_types_htec,
                    htec_contract_types_swss,combined_services,product_banding):

    dataframe = df.copy()
    ## --------------------------------------------- IB
    if type == 'ib':

        ib_cols = ['ID', 'Name', 'Best Site ID', 'Best Site Customer Name',
                    'Best Site Sales Level 2 Name', 'Coverage', 'Contract Number', 'Covered Line Status', 'Contract Type', 'Contract Line End Quarter',
                    'Contract Line End Fiscal Year', 'Instance Shipped Fiscal Year', 'Service Brand Code', 'Offer Type Name', 'Asset Type',
                    'LDoS', 'LDoS Fiscal Quarter', 'LDoS FY', 'Business Entity Name', 'Sub Business Entity Name', 'Product Family',
                    'Product ID', 'Product Type', 'SWSS Flag', 'Default Service List Price USD', 'Item Quantity',
                    'Annual Extended Contract Line List USD Amount', 'Annual Contract Line Net USD Amount',
                    'Annualized Extended Contract Line List USD Amount', 'Annualized Contract Line Net USD Amount',
                    'Contract Line List Price USD', 'Contract Line Net Price USD', 'Asset List Amount']

        dataframe.columns = ib_cols

        dataframe = dataframe.astype({'ID': int,
                            'Name': str,
                            'Best Site ID': float,  # double
                            'Best Site Customer Name': str,
                            'Best Site Sales Level 2 Name': str,
                            'Coverage': str,
                            'Contract Number': int,  # big_int
                            'Covered Line Status': str,
                            'Contract Type': str,
                            'Contract Line End Quarter': int,
                            'Contract Line End Fiscal Year': int,
                            'Instance Shipped Fiscal Year': int,
                            'Service Brand Code': str,
                            'Offer Type Name': str,
                            'Asset Type': str,
                            #'LDoS':datetime,
                            'LDoS Fiscal Quarter': str,
                            'LDoS FY': int,
                            'Business Entity Name': str,
                            'Sub Business Entity Name': str,
                            'Product Family': str,
                            'Product ID': str,
                            'Product Type': str,
                            'SWSS Flag': str,
                            'Default Service List Price USD': float,
                            'Item Quantity': float,
                            'Annual Extended Contract Line List USD Amount': float,
                            'Annual Contract Line Net USD Amount': float,
                            'Annualized Extended Contract Line List USD Amount': float,
                            'Annualized Contract Line Net USD Amount': float,
                            'Contract Line List Price USD': float,
                            'Contract Line Net Price USD': float,
                            'Asset List Amount': float})

        dataframe['LDoS'] = dataframe['LDoS'].apply(lambda x: datetime.strptime(str(x) , '%Y-%m-%d %H:%M:%S') if pd.notna(x) else x)

        dataframe = dataframe.merge(contract_types_list, how='left', left_on='Contract Type',right_on='Contract Type', suffixes=('', ' (SNTCSolutionSupport)'))
        dataframe = dataframe.merge(success_track_pricing_list, how='left', left_on='Product ID',right_on='Product SKU', suffixes=('', ' (Success Track PIDs)'))
        dataframe = dataframe.merge(sspt_pricing_list_eligibleSSPT, how='left',left_on='Product ID', right_on='Product SKU', suffixes=('', ' (Eligible SSPT)'))
        dataframe = dataframe.merge(sspt_pricing_list_outputTable, how='left',left_on='Product ID', right_on='Product SKU', suffixes=('', ' (Output Table)'))
        dataframe = dataframe.merge(sntc_pricing_list, how='left', left_on='Product ID',right_on='Product SKU ', suffixes=('', ' (Output)'))
        dataframe = dataframe.merge(htec_contract_types_swss, how='left',left_on='Contract Type', right_on='GSP', suffixes=('', ' (SWSS)'))
        dataframe = dataframe.merge(htec_contract_types_htec, how='left',left_on='Contract Type', right_on='GSP', suffixes=('', ' (HTEC)'))
        dataframe = dataframe.merge(combined_services, how='left',left_on='Contract Type', right_on='Combined Services Service Level')
        dataframe = dataframe.merge(product_banding, how='left', left_on='Product Family', right_on='INTERNAL_BE_PRODUCT_FAMILY')
        dataframe.to_csv(folder_path, index=False)

    ## --------------------------------------------- Coverage

    elif type == 'coverage':

        coverage_cols = ['ID', 'Name', 'Coverage', 'Item Quantity',
                        'Asset List Amount', 'Asset Net Amount', 'Annual Contract Line Net USD Amount',
                        'Contract Line Net Price USD', 'Annualized Extended Contract Line List USD Amount'
        ]

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
    '''
    La funcion recibe la ruta de un archivo excel y calcula el IB value
    entradas:
        path: Ruta del archivo.
    salidas:
        
    '''

    #Leyendo archivo de IB
    merge = IB

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
        #merge[['LDoS','LDoS Flag']][merge['LDoS Flag']=='N']

        def select_data_to_display(LDoS_flag,cover_lin_status,eligible,coverage):
            value = ''
            if (LDoS_flag =='Y') & (cover_lin_status != 'TERMINATED') & (eligible=='YES') & (coverage=='NOT COVERED'):
                value='Uncovered Opportunity'
            elif  (eligible=='YES') & (coverage=='COVERED'):
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

        merge4=merge[(merge['SNTC SSPT Offer Flag']== 'CISCO SWSS')]
        merge5=merge[(merge['SNTC SSPT Offer Flag']=='Combined Services')]
        merge6=merge[(merge['SNTC SSPT Offer Flag']=='Partner Support')]
        merge7=merge[(merge['SNTC SSPT Offer Flag']=='Smartnet Total Care')]
        merge8=merge[(merge['SNTC SSPT Offer Flag']=='SP Base')]
        merge9=merge[(merge['SNTC SSPT Offer Flag']=='SS PLUS SWSS')] 
        merge11=merge[(merge['SNTC SSPT Offer Flag']=='TELEPRESENCE CUSTOMERS')]

        merge=pd.concat([merge4,merge5,merge6,merge7,merge8,merge9,merge11])

        #filtro product banding
        merge12=merge[merge["ADJUSTED_CATEGORY"]=='High']
        merge13=merge[merge["ADJUSTED_CATEGORY"]=='Medium']
        
        merge=pd.concat([merge12,merge13])

        #filtro upsell to sspt
        merge=merge[merge['Convertsto']!=0]

        #Success Track: All
        #Arquitecture: All
        #Product Family: All

        # Calculando - CS Multiplier SL
        merge['CS Multiplier SL'] = ''
        merge = merge.reset_index()
        
        if merge.shape[0] != 0:
            def service_level(ib):
                
                ib['CS Multiplier SL'] = ib[['Service Level','CS Multiplier SL','SSS2P']].apply(lambda x: x[2] if x[0] == '24x7x2' else x[1], axis=1)
                ib['CS Multiplier SL'] = ib[['Service Level','CS Multiplier SL','SSC2P']].apply(lambda x: x[2] if x[0] == '24x7x2OS' else x[1], axis=1)
                ib['CS Multiplier SL'] = ib[['Service Level','CS Multiplier SL','SSSNP']].apply(lambda x: x[2] if x[0] == '24x7x4' else x[1], axis=1)
                ib['CS Multiplier SL'] = ib[['Service Level','CS Multiplier SL','SSC4P']].apply(lambda x: x[2] if x[0] == '24x7x4OS' else x[1], axis=1)
                ib['CS Multiplier SL'] = ib[['Service Level','CS Multiplier SL','SSSNE']].apply(lambda x: x[2] if x[0] == '8x5x4' else x[1], axis=1)
                ib['CS Multiplier SL'] = ib[['Service Level','CS Multiplier SL','SSC4S']].apply(lambda x: x[2] if x[0] == '8x5x4OS' else x[1], axis=1)
                ib['CS Multiplier SL'] = ib[['Service Level','CS Multiplier SL','SSSNT']].apply(lambda x: x[2] if x[0] == 'NBD' else x[1], axis=1) 
                ib['CS Multiplier SL'] = ib[['Service Level','CS Multiplier SL','SSCS']].apply(lambda x: x[2] if x[0] == '8x5xNDBOS' else x[1], axis=1)       
                ib['CS Multiplier SL'] = ib[['Service Level','CS Multiplier SL','SSDR7']].apply(lambda x: x[2] if x[0] == 'DR 24x7x4OS' else x[1], axis=1)
                ib['CS Multiplier SL'] = ib[['Service Level','CS Multiplier SL','SSDR5']].apply(lambda x: x[2] if x[0] == 'DR 8x5xNDBOS' else x[1], axis=1)
                ib['CS Multiplier SL'] = ib[['Service Level','CS Multiplier SL','SSSW']].apply(lambda x: x[2] if x[0] == 'SNTC NO RMA' else x[1], axis=1)
                ib['CS Multiplier SL'] = ib[['Service Level','CS Multiplier SL','SSSW']].apply(lambda x: 0 if x[0] not in ['24x7x2','24x7x2OS','24x7x4','24x7x4OS','8x5x4','8x5x4OS','NBD','8x5xNDBOS','DR 24x7x4OS','DR 8x5xNDBOS','SNTC NO RMA'] else x[1], axis=1)

            service_level(merge)
        
            # Calculando - CS SSPT (Multiplier)
            merge['CS SSPT (Multiplier)'] = 0
            merge['CS SSPT (Multiplier)'] = merge[['Uplift','CS SSPT (Multiplier)','CS Multiplier SL','Item Quantity','Multiplier']].apply(lambda x: 0 if x[0] == '' else x[2]*x[3]*x[4], axis=1)

        # Calculando - Uplift Recommended SL $
        
            merge['Uplift Recommended SL $']= ''
            lista = ['SNCO','PSUP','C2P','3C4P','C4P','C4S','CS','UCSD5','UCSD7','S2P','SNTE','SNTP','3SNT','5SNT','SNT','PSRT','PSUT','LSNT']
            lista2 = ['SPAR1','SPAR2','SPAR3','SPAR4','SPC2P','SBAR1','SPCS']
            lista3 = ['ECDN','ECDO']

            merge = merge.reset_index()
        
            def uplift_recommended_sl(contract_type,item_quantity,ssc2p,ssc4p,ssc4s,sscs,ssdr5,
                                ssdr7,sss2p,sssne,sssnp,sssnt,sssw,ecmus,sntc_sspt_offer_flag,cs_sspt_multiplier,
                                ssnco,sssnc):
                value = 0

                if (contract_type == 'C2P') & (ssc2p != 0):
                    value = ssc2p*item_quantity
                elif (contract_type == '3C4P') & (ssc4p != 0):
                            value = ssc4p*item_quantity    
                elif (contract_type == 'C4P') & (ssc4p != 0):
                    value = ssc4p*item_quantity
                elif (contract_type == 'C4S') & (ssc4s != 0):
                    value = ssc4s*item_quantity
                elif (contract_type == 'CS') & (sscs != 0):
                    value = sscs*item_quantity
                elif (contract_type == 'UCSD5') & (ssdr5 != 0):
                    value = ssdr5*item_quantity    
                elif (contract_type == 'UCSD7') & (ssdr7 != 0):
                    value = ssdr7*item_quantity
                elif (contract_type == 'S2P') & (sss2p != 0):
                    value = sss2p*item_quantity
                elif (contract_type == 'SNTE') & (sssne != 0):
                    value = sssne*item_quantity
                elif (contract_type == '3SNTP') & (sssnp != 0):
                    value = sssnp*item_quantity
                elif (contract_type == 'SNTP') & (sssnp != 0):
                    value = sssnp*item_quantity
                elif (contract_type == '5SNTP') & (sssnp != 0):
                    value = sssnp*item_quantity
                elif (contract_type == 'SW') & (sssnt != 0):
                    value = sssnt*item_quantity   
                elif (contract_type == 'SW') & (sssw != 0):
                    value = sssw*item_quantity
                elif (contract_type == 'ECMU') & (ecmus != 0):
                    value = ecmus*item_quantity
                elif (sntc_sspt_offer_flag == 'Combined Services'):
                    value = cs_sspt_multiplier   
                elif (contract_type == 'PSUP') & (sssnp != 0):
                    value = sssnp*item_quantity    
                elif (contract_type == 'SNCO') & (ssnco != 0):
                    value = ssnco*item_quantity 
                elif (contract_type == 'SNC') & (sssnc != 0):
                    value = sssnc*item_quantity 
                elif (contract_type == 'SPAR2') & (sntc_sspt_offer_flag == 'SP Base')& (sssne != 0):
                    value = sssne*item_quantity     
                elif (contract_type == 'SPAR3') & (sntc_sspt_offer_flag == 'SP Base')& (sssnp != 0):
                    value = sssnp*item_quantity 
                elif (contract_type == 'SPAR4') & (sntc_sspt_offer_flag == 'SP Base')& (sss2p != 0):
                    value = sss2p*item_quantity
                elif (contract_type == 'SPC2P') & (sntc_sspt_offer_flag == 'SP Base')& (ssc2p != 0):
                    value = ssc2p*item_quantity
                elif (contract_type == 'SPC4P') & (sntc_sspt_offer_flag == 'SP Base'):
                    value = ssc4p*item_quantity
                elif (contract_type == 'SPCS') & (sntc_sspt_offer_flag == 'SP Base')& (sscs != 0):
                    value = sscs*item_quantity
                elif (contract_type == 'ECDN') & (sntc_sspt_offer_flag == 'TELEPRESENCE CUSTOMERS'):
                    value = sssnt*item_quantity         
                elif (contract_type == 'ECDO') & (sntc_sspt_offer_flag == 'TELEPRESENCE CUSTOMERS'):
                    value = sscs*item_quantity          
                elif (contract_type in lista )& (sssnt != 0):
                    value = sssnt*item_quantity
                elif (contract_type in lista2) & (sntc_sspt_offer_flag == 'SP Base')& (sssnt != 0):
                    value = sssnt*item_quantity    
                else:
                    value = 0

                return value

            merge['Uplift Recommended SL $'] = merge[['Contract Type','Item Quantity','SSC2P','SSC4P','SSC4S','SSCS','SSDR5',
                                                    'SSDR7','SSS2P','SSSNE','SSSNP','SSSNT','SSSW','ECMUS','SNTC SSPT Offer Flag',
                                                    'CS SSPT (Multiplier)','SSNCO','SSSNC'
                                                    ]].apply(lambda x: uplift_recommended_sl(x[0], x[1], x[2], x[3], x[4], x[5], x[6], x[7], x[8], x[9], x[10], 
                                                                                            x[11], x[12], x[13], x[14], x[15],x[16],x[17]), axis=1)
        
        else:
            merge['Uplift Recommended SL $'] = 0

        merge2 = merge

        SP_Oppty1 = round(((merge2['Uplift Recommended SL $'].sum()) - (merge2['Annualized Extended Contract Line List USD Amount'].sum()))/1000,1)

        return SP_Oppty1

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
    merge1 = merge1.rename(columns={'Product SKU':'Success Tracks'})
    merge1['Success Tracks'] = merge1['Success Tracks'].fillna('Not Elegible')

    if len(merge1) == 0:
        return 'N/A'
    else:
        j=merge1.iloc[0,0]  
        #-----------------------------------------------------------------------------------------------------------------
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
        #merge1['ST Estimated List Price'] = (merge1['L2NBD'] + merge1['L2SWT'])*merge1['Item Quantity']
        merge1['ST Estimated List Price'] = merge1['L2NBD']*merge1['Item Quantity']

        #------------------------------------------------------Filtering------------------------------------------------------------------
        merge1 = merge1[merge1['Coverage'] == 'COVERED']
        merge1 = merge1[merge1['Success Tracks'] == 'Elegible']
        #filtro product banding
        merge12=merge1[merge1["ADJUSTED_CATEGORY"]=='High']
        merge13=merge1[merge1["ADJUSTED_CATEGORY"]=='Medium']
        
        merge1=pd.concat([merge12,merge13])

        #----------------------------------------Calculating Total ST Oppty KPI------------------------------------------------------------

        sn_nbd_st_e = merge1['SmartNet NBD ST Eligible'].sum()
        sn_st_not_covered = merge1['SmartNet ST Not Covered'].sum()
        st_e_list_price = merge1['ST Estimated List Price'].sum()
        ST_Oppty1= round (((st_e_list_price - sn_st_not_covered - sn_nbd_st_e)/1000),1)
       

        return ST_Oppty1


#Calculation of Expert Care oppty

def expert_care_verification(expert_care):
    df = expert_care.copy()
    df = df[df['IB Bands'] == 'IB Value $1M - $31M']
    oppty = df['List Price (USD)'].sum()
    return oppty


#Calculation of smartnet value

def smartnet_verification(ib):
    df = ib.copy()
    oppty = df['Default Service List Price USD'].sum()
    return oppty


#Calculation of IB value, IB covered and Mayor Renewal 

def IB_attributes(IB):
    
# ------------------------------------------------------------------Calculating IB-----------------------------------------------------------------------------------
    data = IB.groupby([IB['ID'],IB['Coverage'][IB['Coverage']=='COVERED']])[['Asset List Amount']].sum()
    data = data.reset_index()
    data = data.rename(columns={'Asset List Amount':'IB Value'})
    data = data.drop(labels='Coverage', axis=1)
    data.fillna(0,inplace=True)
    

# ------------------------------------------------------------------Calculating % coverage------------------------------------------------------------------------------
    a = IB.groupby([IB['ID']])[['Asset List Amount']].sum()
    a = a.reset_index()
    a = a.rename(columns={'Asset List Amount':'IB Total'})
    data= data.merge(a,right_on="ID",left_on="ID", how='left')
    data.fillna(0,inplace=True)
    data['Coverage Percentage'] = (data['IB Value']/data['IB Total'])*100
    data = data.drop(labels='IB Total', axis=1)
    

#---------------------------------------------------------------Calculating Mayor Renewal -----------------------------------------------------------------------------

    IB['Annual Extended Contract Line List USD Amount'] = IB['Annual Extended Contract Line List USD Amount'].astype(float) 
    g = IB.groupby([IB['ID'],IB['Contract Line End Quarter']])['Annual Extended Contract Line List USD Amount'].sum()
    g = g.reset_index()
    g['Contract Line End Quarter'] = g['Contract Line End Quarter'].astype(str)
    h = g.groupby(g['ID'])[['Annual Extended Contract Line List USD Amount']].max()

    lis = h['Annual Extended Contract Line List USD Amount'].to_list()
    lis2 = []
    for i in lis:
        j = g[g['Annual Extended Contract Line List USD Amount'] == i].iloc[0,1]
        lis2.append(j)


    df_mayor_ren=h.reset_index()
    df_mayor_ren['Mayor Renewal'] = lis2
    df_mayor_ren = df_mayor_ren.drop(labels='Annual Extended Contract Line List USD Amount', axis=1)
    data= data.merge(df_mayor_ren,right_on="ID",left_on="ID", how='left')
    data.fillna(0,inplace=True)

    return data


#Assign color function for Q&A dataframe

def color_qa(value):
    if value in ['Incorrect', 'Negative Value','Empty Value', 'Big value','QA Package Info', 'No IB data', 'No data']:
        color = 'red'
    else:
        color = 'green'

    return 'color: %s' % color


#Validation checking for negative or empty IB value

def ib_value_validation(data):    
    try:
        if data['IB Value'][0]<0:
            return('Incorrect')
        else:
            return("Correct")
    except:
        return("No IB data")
   

#Validation of coverage percentage between the correct range

def ib_covered_validation(data):
    try:
        if (data['Coverage Percentage'][0]/100 >= 0) & (data['Coverage Percentage'][0]/100 <= 1):
            return('Correct')
        else: 
            return('Incorrect')
    except:
        return('No IB data')


#Validation for empty values of Mayor Renewal 

def rw_validation(data):
    try:
        if data['Mayor Renewal'][0] == '':
            return('Empty Value')
        else:
            return('Correct')
    except:
        return('No IB data')


#Validation for negative values 

def oppty_validation(oppty):
    try:
        if oppty >= 0:
            return('Correct')
        else:
            return('Negative Value')
    except:
        return('No data')

#Calculation of smartnet value for PI's eligibles for Success Tracks

def smartnet_total_care_NBD_list_price(ib):
    ib = ib[~(pd.isna(ib['Product SKU']) | ib['Contract Type'].isin(['L1NB3','L1NB5','L1NBD','L1SWT','L24H3','L24H5','L24HR','L2NB3','L2NB5','L2NBD','L2SWT']))]
    ib = ib[ib['ADJUSTED_CATEGORY'].isin(['High','Medium'])]
    ib = ib[ib['Coverage'] == 'COVERED']
    smartnet_value = int(ib['Annualized Extended Contract Line List USD Amount'].sum())
    lent = len(str(smartnet_value))
    return smartnet_value, lent


#Calculation of ST level 2 opportunity 

def estimated_list_price(ib):
    ib = ib[~(pd.isna(ib['Product SKU']) | ib['Contract Type'].isin(['L1NB3','L1NB5','L1NBD','L1SWT','L24H3','L24H5','L24HR','L2NB3','L2NB5','L2NBD','L2SWT']))]
    ib = ib[ib['ADJUSTED_CATEGORY'].isin(['High','Medium'])]
    ib = ib[ib['Coverage'] == 'COVERED']
    l2_swt = ib['L2SWT'].fillna(0) * ib['Item Quantity'].fillna(0)
    estimated_value = int(sum((ib['L2NBD'].fillna(0) + l2_swt) * ib['Item Quantity'].fillna(0)))
    lent = len(str(estimated_value))
    return estimated_value, lent


#Validation of lenght for calculated values

def lenght_validation(number, lenght=8):
    try:
        if number >= lenght:
            return('Big value')
        else:
            return('Correct')
    except:
        return('No data')



def smartsheet_len_info(df):
    
    parties = df['Who should be notified on completion of Analysis'].iloc[0].split(',')
    appliance = df['Appliance ID'].iloc[0].split(',')
    inventory = df['Inventory Name'].iloc[0].split(',')
    
    if (len(df['sav_list'].iloc[0]) > 11)  | (len(df['gu_list'].iloc[0]) > 11) | (len(df['cav_list'].iloc[0]) > 11) \
        |(len(df['contract_list'].iloc[0]) > 11) | (len(parties) > 11) | (len(appliance) > 11) | (len(inventory) > 11):
        return 'QA Package Info'
    else:
        return 'Correct'
    

