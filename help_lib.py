from IPython.display import Image
import os

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

class help_functions:
    error_image = False
    my_path = str(os.getcwd()) + "\help_images\\"
    my_path = my_path.replace("\\", "/")
    
    def load_image(self, path:str):
        self.path = path
        self.path = self.path.replace("\\", "/")
        try:
            self.picture = Image(filename = self.path)
            return self.picture
        except:
            help_functions.error_image = True

    def my_print(self,function_name,origin_library,embeded_function_name,embeded_function_name_origin_libary,
                description,parameters,output,additional_libraries,additional_resources,links):
        self.function_name = function_name
        self.origin_library = origin_library
        self.embeded_function_name = embeded_function_name
        self.embeded_function_name_origin_libary = embeded_function_name_origin_libary
        self.description = description
        self.parameters = parameters
        self.output = output
        self.additional_libraries = additional_libraries
        self.additional_resources = additional_resources
        self.links = links

        print(bcolors.OKBLUE + bcolors.BOLD + "FUNCTION NAME: " + bcolors.ENDC + self.function_name +"\n" + bcolors.OKBLUE + bcolors.BOLD + "ORIGIN LIBRARY: " + bcolors.ENDC + self.origin_library + "\n" +
        bcolors.OKBLUE + bcolors.BOLD + "EMBEDED FUNCTION: " + bcolors.ENDC + self.embeded_function_name + "\n" + bcolors.OKBLUE + bcolors.BOLD + "EMBEDED FUNCTION ORIGIN LIBRARY: " + bcolors.ENDC + self.embeded_function_name_origin_libary + "\n" +
        bcolors.OKBLUE + bcolors.BOLD + "DESCRIPTION: " + bcolors.ENDC + self.description + "\n" + bcolors.OKBLUE + bcolors.BOLD + "PARAMETER(S): " + bcolors.ENDC + self.parameters +"\n" + 
        bcolors.OKBLUE + bcolors.BOLD + "OUTPUT: " + bcolors.ENDC + self.output +"\n" + bcolors.OKBLUE + bcolors.BOLD + "ADDITIONAL LIBRARIES: " + bcolors.ENDC + self.additional_libraries +"\n" +
        bcolors.OKBLUE + bcolors.BOLD + "ADDITIONAL RESOURCES: " + bcolors.ENDC + self.additional_resources + "\n" + bcolors.OKBLUE + bcolors.BOLD + "LINKS: " + bcolors.ENDC + self.links + "\n" + "\n" +
        bcolors.OKBLUE + bcolors.BOLD + f"LINE ON THE CODE WHERE {self.function_name} APPEARS FOR THE FIRST TIME:"+ bcolors.ENDC)
        if help_functions.error_image == True: print(bcolors.FAIL + "ERROR LOADING THE PICTURE. Make sure you have the 'help_images' folder updated." + bcolors.ENDC)

    def my_print_errors(self,error_name,solution,founded_in):
                
        self.error_name = error_name
        self.solution = solution
        self.founded_in = founded_in

        print(bcolors.OKBLUE + bcolors.BOLD + "ERROR NAME: " + bcolors.ENDC + self.error_name +"\n" + bcolors.OKBLUE + bcolors.BOLD + "SOLUTION: " + bcolors.ENDC + self.solution + "\n" +
        bcolors.OKBLUE + bcolors.BOLD + "FOUNDED IN:" + bcolors.ENDC + self.founded_in)
        if help_functions.error_image == True: print(bcolors.FAIL + "ERROR LOADING THE PICTURE. Make sure you have the 'help_images' folder updated." + bcolors.ENDC)
    

class cr(help_functions):

    def cr_flowchart(self):
        self.function_name = self.cr_flowchart.__name__
        self.origin_library = "help_lib.py"
        self.embeded_function_name = "NA"
        self.embeded_function_name_origin_libary = "NA"
        self.description = "You can use this flowchart to better review the current Coverage Reports creation process."
        self.parameters = "NA"
        self.output = "A picture with the flowchart for the coverage report will be displayed with this function."
        self.picture = help_functions.load_image(self, f"{self.my_path}Coverage Flowchart.png")
        self.additional_libraries = "NA"
        self.additional_resources = "Please refer to following code to get additional information: 'help(parameter_name)'"
        self.links = "You can find the full-size image at: " + (bcolors.UNDERLINE + self.my_path + "Coverage Flowchart.png" + bcolors.ENDC)
        help_functions.my_print(self, self.function_name,self.origin_library,self.embeded_function_name,self.embeded_function_name_origin_libary,
        self.description,self.parameters,self.output,self.additional_libraries,self.additional_resources,self.links)
        return self.picture
        
    def init_conn(self):
        self.function_name = self.init_conn.__name__
        self.origin_library = "smartsheet_lib.py"
        self.embeded_function_name = "NA"
        self.embeded_function_name_origin_libary = "NA"
        self.description = "This function sets a connection with the SmartSheet API by using the access token created for the OP INTAKE REQUEST FIELD tracker."
        self.parameters = """
        1. token: Personal access tokens are long-lived authentication tokens that allow users to run automation with SmartSheet APIs without requiring hard-coded credentials or interactive login.
        """
        self.output = "This function will get you connected with Smartsheet which is stored in the variable 'smartsheet_client'."
        self.picture = help_functions.load_image(self, f"{self.my_path}init_conn.png")
        self.additional_libraries = """
        1. Smartsheet from smartsheet
        """
        self.additional_resources = "Please refer to following code to get additional information: 'help(parameter_name)'"
        self.links = """
        1. How to connect to Smartsheet: https://smartsheet-platform.github.io/api-docs/
        """
        help_functions.my_print(self, self.function_name,self.origin_library,self.embeded_function_name,self.embeded_function_name_origin_libary,
        self.description,self.parameters,self.output,self.additional_libraries,self.additional_resources,self.links)
        return self.picture

    def load_sheet(self):
        self.function_name = self.load_sheet.__name__
        self.origin_library = "smartsheet_lib.py"
        self.embeded_function_name = "NA"
        self.embeded_function_name_origin_libary = "NA"
        self.description = """
        This function loads the OP Intake Request Fields from Smartsheet using the sheet ID, the connection previously done with the
        function 'init_conn', and a date. This sheet contains information about the accounts, Offer Estimator/Coverage Report, Data Analysts
        who made the reports, and who requested them.
        """
        self.parameters = """
        1. sheet_id: ID from the sheet wanted to be loaded.
        2. client: object/variable that contains the connection to Smartsheet.
        3. modified_since: date of modification.
        """
        self.output = "Sheet model object. It comes as JSON that includes info about the sheet columns and metadata describing some configurations."
        self.picture = help_functions.load_image(self,f"{self.my_path}load_sheet1.png")
        self.additional_libraries = "NA"
        self.additional_resources = "Please refer to following code to get additional information: 'help(parameter_name)'"
        self.links = """
        1. Smartsheet documentation: https://smartsheet-platform.github.io/api-docs/
        """
        help_functions.my_print(self, self.function_name,self.origin_library,self.embeded_function_name,self.embeded_function_name_origin_libary,
        self.description,self.parameters,self.output,self.additional_libraries,self.additional_resources,self.links)
        return self.picture

    def get_last_n_rows(self):
        self.function_name = self.get_last_n_rows.__name__
        self.origin_library = "smartsheet_lib.py"
        self.embeded_function_name = "NA"
        self.embeded_function_name_origin_libary = "NA"
        self.description = "This function retrieves the last 'n' rows from the OP Intake Request Fields sheet. To do this, the function receives the connection to Smartsheet and the number of rows to retrieve."
        self.parameters = """
        1. sheet: JSON object that contains the information about the sheet loaded previously.
        2. n_rows: number of rows to retrieve. These rows are retrieved bottom-up.
        """ 
        self.output = "A list of row models from Smartsheet."
        self.picture = help_functions.load_image(self,f"{self.my_path}get_last_n_rows.png")
        self.additional_libraries = "NA"
        self.additional_resources = "Please refer to following code to get additional information: 'help(parameter_name)'"
        self.links = "NA"
        help_functions.my_print(self, self.function_name,self.origin_library,self.embeded_function_name,self.embeded_function_name_origin_libary,
        self.description,self.parameters,self.output,self.additional_libraries,self.additional_resources,self.links)
        return self.picture

    def sheet_to_df2(self):
        self.function_name = self.sheet_to_df2.__name__
        self.origin_library = "smartsheet_lib.py"    
        self.embeded_function_name = "NA"
        self.embeded_function_name_origin_libary = "NA"
        self.description = "This function creates a Pandas DataFrame based on the rows and columns retrieved by the functions 'load_sheet' and 'get_last_n_rows' respectively."
        self.parameters = """
        1. sheet: variable that stores rows retrieved.
        2. columns: columns from the variable that stores the JSON sheet object.
        """ 
        self.output = "DataFrame with the information retrieved from the OP Intake Request Fields sheet."
        self.picture = help_functions.load_image(self,f"{self.my_path}help_images\sheet_to_df2.png")
        self.additional_libraries = """
        1. DataFrame from pandas. 
        """
        self.additional_resources = "Please refer to following code to get additional information: 'help(parameter_name)'"
        self.links = """
        1. DataFrame official documentation: https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.html
        """
        help_functions.my_print(self, self.function_name,self.origin_library,self.embeded_function_name,self.embeded_function_name_origin_libary,
        self.description,self.parameters,self.output,self.additional_libraries,self.additional_resources,self.links)
        return self.picture

    def get_da_requests(self):
        self.function_name = self.get_da_requests.__name__
        self.origin_library = "utils_coverage.py / utils_lib.py"
        self.embeded_function_name = "clean_region"
        self.embeded_function_name_origin_libary = "utils_coverage.py / utils_lib.py"   
        self.description = """
        This function filters the information retrieved from Smartsheet by a specific Data Analyst responsible for creating the report and defining a DataFrame.
        During the creation of the Dataframe, the format of a few columns is defined depending on the data type.
        """
        self.parameters = """
        1. da: a variable that stores your Cisco e-mail to get authenticated.  
        2. df: DataFrame based on the information retrieved from the OP Intake Request Fields in Smartsheet.
        """ 
        self.output = "DataFrame with the information retrieved from the OP Intake Request Fields sheet filtered by a specific Data Analyst."
        self.picture = help_functions.load_image(self,f"{self.my_path}get_da_requests.png")
        self.additional_libraries = """
        1. DataFrame from pandas. 
        """
        self.additional_resources = "Please refer to following code to get additional information: 'help(parameter_name)'"
        self.links = "NA"
        help_functions.my_print(self, self.function_name,self.origin_library,self.embeded_function_name,self.embeded_function_name_origin_libary,
        self.description,self.parameters,self.output,self.additional_libraries,self.additional_resources,self.links)
        return self.picture

    def get_ids_list(self):
        self.function_name = self.get_ids_list.__name__
        self.origin_library = "utils_coverage.py / utils_lib.py"
        self.embeded_function_name = "NA"
        self.embeded_function_name_origin_libary = "NA"
        self.description = """
        This function takes the DataFrame with the information from the OP Intake Request Fields in Smartsheet, and converts the column that contains a type ID list into a 
        string depending on if the account has an ID TYPE in particular (SAV, CAV, GU, CR Party).
        """
        self.parameters = """
        1. fields_df: Dataframe with the information from the OP Intake Request Fields in Smartsheet.
        2. separator: indicates a grammatical mark or symbol to separate each element in a character string.
        """ 
        self.output = "A character string(s) with the corresponding ID Type(s) separated by a semicolon."
        self.picture = help_functions.load_image(self,f"{self.my_path}get_ids_list.png")
        self.additional_libraries = """
        1. chain from itertools. 
        """
        self.additional_resources = "Please refer to following code to get additional information: 'help(parameter_name)'"
        self.links = """
        1. itertools documentation: https://docs.python.org/3/library/itertools.html
        """
        help_functions.my_print(self, self.function_name,self.origin_library,self.embeded_function_name,self.embeded_function_name_origin_libary,
        self.description,self.parameters,self.output,self.additional_libraries,self.additional_resources,self.links)
        return self.picture
        
    def get_uncovered_data(self):
        self.function_name = self.get_uncovered_data.__name__
        self.origin_library = "utils_coverage.py"    
        self.embeded_function_name = "NA"
        self.embeded_function_name_origin_libary = "NA"
        self.description = """
        This function retrieves accounts information where the coverage attribute from Snowflake is 'NOT COVERED'. The search is filtered by an ID(s) and ID(s) Type.
        All the attributes retrieved from Snowflake are renamed within 'get_uncovered_data' function for a better understanding.
        """
        self.parameters = """
        1. user: a variable that stores your Cisco e-mail to get authenticated.
        2. ids_sav: string with a SAV ID list from the account(s) being processed.
        3. ids_gu: string with a GU ID list from the account(s) being processed.
        4. ids_cr: string with a CR PaRty ID list from the account(s) being processed.
        5. ids_cav: string with a CAV ID list from the account(s) being processed.
        """ 
        self.output = "A DataFrame with uncovered data retrieved from a Snowflake table."
        self.picture = help_functions.load_image(self,f"{self.my_path}get_uncovered_data.png")
        self.additional_libraries = """
        1. Connector from snowflake.
        2. DataFrame from pandas.
        """
        self.additional_resources = "Please refer to following code to get additional information: 'help(parameter_name)'"
        self.links = """
        1. Using Python Connector for Snowflake: https://docs.snowflake.com/en/user-guide/python-connector-example.html
        """
        help_functions.my_print(self, self.function_name,self.origin_library,self.embeded_function_name,self.embeded_function_name_origin_libary,
        self.description,self.parameters,self.output,self.additional_libraries,self.additional_resources,self.links)
        return self.picture

    def get_coverage_data(self):
        self.function_name = self.get_coverage_data.__name__
        self.origin_library = "utils_coverage.py"    
        self.embeded_function_name = "NA"
        self.embeded_function_name_origin_libary = "NA"
        self.description = """
        This function retrieves coverage information from Snowflake looking for an ID(s) and ID(s) Type. This data shows the architectures, products covered,
        prices and other attributes of the client. All the attributes retrieved from Snowflake are renamed within 'get_coverage_data' function for a better understanding.
        """
        self.parameters = """
        1. user: a variable that stores your Cisco e-mail to get authenticated.
        2. ids_sav: string with a SAV ID list from the account(s) being processed.
        3. ids_gu: string with a GU ID list from the account(s) being processed.
        4. ids_cr: string with a CR PaRty ID list from the account(s) being processed.
        5. ids_cav: string with a CAV ID list from the account(s) being processed.
        """ 
        self.output = "A DataFrame with coverage data retrieved from a Snowflake table."
        self.picture = help_functions.load_image(self,f"{self.my_path}get_coverage_data.png")
        self.additional_libraries = """
        1. Connector from snowflake.
        2. DataFrame from pandas.  
        """
        self.additional_resources = "Please refer to following code to get additional information: 'help(parameter_name)'"
        self.links = """
        1. Using Python Connector for Snowflake: https://docs.snowflake.com/en/user-guide/python-connector-example.html
        """
        help_functions.my_print(self, self.function_name,self.origin_library,self.embeded_function_name,self.embeded_function_name_origin_libary,
        self.description,self.parameters,self.output,self.additional_libraries,self.additional_resources,self.links)
        return self.picture

    def get_contracts_data(self):
        self.function_name = self.get_contracts_data.__name__
        self.origin_library = "utils_coverage.py"    
        self.embeded_function_name = "NA"
        self.embeded_function_name_origin_libary = "NA"
        self.description = """
        This function retrieves contracts information from Snowflake looking for an ID(s) and ID(s) Type. This data shows the architectures, contract type,
        flags and other attributes of the client. All the attributes retrieved from Snowflake are renamed within 'get_contracts_data' function for a better understanding.
        """
        self.parameters = """
        1. user: a variable that stores your Cisco e-mail to get authenticated.
        2. ids_sav: string with a SAV ID list from the account(s) being processed.
        3. ids_gu: string with a GU ID list from the account(s) being processed.
        4. ids_cr: string with a CR PaRty ID list from the account(s) being processed.
        5. ids_cav: string with a CAV ID list from the account(s) being processed.
        """ 
        self.output = "A DataFrame with contracts data retrieved from a Snowflake table."
        self.picture = help_functions.load_image(self,f"{self.my_path}get_contracts_data.png")
        self.additional_libraries = """
        1. Connector from snowflake.
        2. DataFrame from pandas.  
        """
        self.additional_resources = "Please refer to following code to get additional information: 'help(parameter_name)'"
        self.links = """
        1. Using Python Connector for Snowflake: https://docs.snowflake.com/en/user-guide/python-connector-example.html
        """
        help_functions.my_print(self, self.function_name,self.origin_library,self.embeded_function_name,self.embeded_function_name_origin_libary,
        self.description,self.parameters,self.output,self.additional_libraries,self.additional_resources,self.links)
        return self.picture

    def get_tac_data(self):
        self.function_name = self.get_tac_data.__name__
        self.origin_library = "utils_coverage.py"    
        self.embeded_function_name = "NA"
        self.embeded_function_name_origin_libary = "NA"
        self.description = """
        This function retrieves information from Snowflake looking for an ID(s) and ID(s) Type related with service requests (SRs) of technical issues of the clients. 
        It includes SR's maximum severity, dates, resolution time, channel of receipt, incident status and other attributes. All the attributes retrieved from Snowflake are 
        renamed within 'get_contracts_data' function for a better understanding.
        """
        self.parameters = """
        1. user: a variable that stores your Cisco e-mail to get authenticated.
        2. ids_sav: string with a SAV ID list from the account(s) being processed.
        3. ids_gu: string with a GU ID list from the account(s) being processed.
        4. ids_cr: string with a CR PaRty ID list from the account(s) being processed.
        5. ids_cav: string with a CAV ID list from the account(s) being processed.
        """ 
        self.output = "A DataFrame with SRs data retrieved from a Snowflake table."
        self.picture = help_functions.load_image(self,f"{self.my_path}get_tac_data.png")
        self.additional_libraries = """
        1. Connector from snowflake.
        2. DataFrame from pandas.  
        """
        self.additional_resources = "Please refer to following code to get additional information: 'help(parameter_name)'"
        self.links = """
        1. Using Python Connector for Snowflake: https://docs.snowflake.com/en/user-guide/python-connector-example.html
        """
        help_functions.my_print(self, self.function_name,self.origin_library,self.embeded_function_name,self.embeded_function_name_origin_libary,
        self.description,self.parameters,self.output,self.additional_libraries,self.additional_resources,self.links)
        return self.picture

    def format_columns(self):
        self.function_name = self.format_columns.__name__
        self.origin_library = "utils_coverage.py"    
        self.embeded_function_name = "NA"
        self.embeded_function_name_origin_libary = "NA"
        self.description = """
        This function set the format (known Type Casting in Python) of each column in DataFrames created by the function 'get_uncovered_data', 'get_coverage_data' and 'get_contracts_data'.
        For instance, if data into a DataFrame column is a number but it needs to be treated as a string, here it gets done.
        """
        self.parameters = """
        1. uncovered: DataFrame with uncovered data retrieved from a Snowflake table. 
        2. coverage: DataFrame with coverage data retrieved a from a Snowflake table.
        3. contracts: DataFrame with contracts data retrieved from a Snowflake table.
        """ 
        self.output = "DataFrames 'uncovered', 'coverage' and 'contracts' with casted columns."
        self.picture = help_functions.load_image(self,f"{self.my_path}format_columns.png")
        self.additional_libraries = """
        1. pandas.
        """
        self.additional_resources = "Please refer to following code to get additional information: 'help(parameter_name)'"
        self.links = """
        1. How to Cast with Pandas: https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.astype.html
        """
        help_functions.my_print(self, self.function_name,self.origin_library,self.embeded_function_name,self.embeded_function_name_origin_libary,
        self.description,self.parameters,self.output,self.additional_libraries,self.additional_resources,self.links)
        return self.picture
    #--------------------------------------------------------------------------------------------------------------------------------------------------------------------------

    def get_schema(self):
        self.function_name = self.get_schema.__name__
        self.origin_library = "utils_coverage.py / utils_lib.py"    
        self.embeded_function_name = "NA"
        self.embeded_function_name_origin_libary = "NA"
        self.description = """
        This function creates a SQL table by defining all the attributes that form it. Each of these attributes has a specific data type and a name to identify them in a table representation.
        """
        self.parameters = """
        1. table: given table name to the schema created.
        """ 
        self.output = "Depending on the use case, it will generate a list containing all the attributes that belong to either one of these tables: TAC, Smartsheet, IB, Coverage, SW, CIR"
        self.picture = help_functions.load_image(self,f"{self.my_path}get_schema.png")
        self.additional_libraries = """
        1. Table definition from tableauhyperapi.
        2. SqlType from tableauhyperapi.
        """
        self.additional_resources = "Please refer to following code to get additional information: 'help(parameter_name)'"
        self.links = """
        1. Table Definition: https://help.tableau.com/current/api/hyper_api/en-us/reference/py/_modules/tableauhyperapi/tabledefinition.html
        2. Sqltype: https://help.tableau.com/current/api/hyper_api/en-us/reference/py/_modules/tableauhyperapi/sqltype.html
        """
        help_functions.my_print(self, self.function_name,self.origin_library,self.embeded_function_name,self.embeded_function_name_origin_libary,
        self.description,self.parameters,self.output,self.additional_libraries,self.additional_resources,self.links)
        return self.picture

    def create_extract(self):
        self.function_name = self.create_extract.__name__
        self.origin_library = "utils_coverage.py / utils_lib.py"
        self.embeded_function_name = "get_schema"
        self.embeded_function_name_origin_libary = "utils_coverage.py"
        self.description = """
        This function uses the library 'tableauhyperapi' to start a local Hyper server instance. Here Telemetry is allowed to share information about
        usage data with Tableau and the number of log files are stored before the oldest ones are deleted. In the same way, it is defined as log file size.
        The connection to the Hyper server instance replaces data if exists and creates a schema (file) with a given name.
        
        After the connection is made, a SQL table is defined using the file name created and a set of columns. These columns are named using an embedded function
        called 'get_schema' in which the columns' names and data type are specified. Finally, the database or extract created is inserted using the connection and
        table previously defined.
        """ 
        self.parameters = """
        1. name: a given name to the extract file.
        2. columns: SQL table defined by the function 'get_schema'.
        3. df: TAC DataFrame defined by Type ID.
        4. path: directory where the report (twbx file) is located.
        """ 
        self.output = "NA"
        self.picture = help_functions.load_image(self,f"{self.my_path}create_extract.png")
        self.additional_libraries = """
        1. HyperProcess from tableauhyperapi.
        2. Connection from tableauhyperapi.
        3. TableDefinition from tableauhyperapi.
        """
        self.additional_resources = "Please refer to following code to get additional information: 'help(parameter_name)'"
        self.links = """
        1. HyperProcess: https://help.tableau.com/current/api/hyper_api/en-us/reference/py/_modules/tableauhyperapi/hyperprocess.html
        2. Connection: https://help.tableau.com/current/api/hyper_api/en-us/reference/py/_modules/tableauhyperapi/connection.html
        3. TableDefinition: https://help.tableau.com/current/api/hyper_api/en-us/reference/py/_modules/tableauhyperapi/tabledefinition.html
        """
        help_functions.my_print(self, self.function_name,self.origin_library,self.embeded_function_name,self.embeded_function_name_origin_libary,
        self.description,self.parameters,self.output,self.additional_libraries,self.additional_resources,self.links)
        return self.picture

    def Recommended_Estimate_CR(self):
        self.function_name = self.Recommended_Estimate_CR.__name__
        self.origin_library = "utils_coverage.py"
        self.embeded_function_name = "NA"
        self.embeded_function_name_origin_libary = "NA"
        self.description = """and 
        This function utilizes the uncovered information from the accounts filtered by ID, ID Types, and other flags related to SSPT/SNTC. Finally, a total value 
        is calculated named Total Opportunity shown in the report.
        """ 
        self.parameters = """
        1. uncovered_data_filtered: DataFrame filtered by ID, ID Types, and other flags related to SSPT/SNTC.
        """ 
        self.output = "Total Opportunity value."
        self.picture = help_functions.load_image(self,f"{self.my_path}Recommended_Estimate_CR.png")
        self.additional_libraries = "NA"
        self.additional_resources = "Please refer to following code to get additional information: 'help(parameter_name)'"
        self.links = "NA"
        help_functions.my_print(self, self.function_name,self.origin_library,self.embeded_function_name,self.embeded_function_name_origin_libary,
        self.description,self.parameters,self.output,self.additional_libraries,self.additional_resources,self.links)
        return self.picture

    def get_url(self):
        self.function_name = self.get_url.__name__
        self.origin_library = "utils_coverage.py / utils_lib.py"
        self.embeded_function_name = "NA"
        self.embeded_function_name_origin_libary = "NA"
        self.description = """
        This function takes the customer name and inserts it into a Tableau Server URL to create a customized URL for each report uploaded to Tableau Server.
        """ 
        self.parameters = """
        1. name: customer name.
        """ 
        self.output = "A customized URL for each customer."
        self.picture = help_functions.load_image(self,f"{self.my_path}get_url.png")
        self.additional_libraries = "NA"
        self.additional_resources = "Please refer to following code to get additional information: 'help(parameter_name)'"
        self.links = "NA"
        help_functions.my_print(self, self.function_name,self.origin_library,self.embeded_function_name,self.embeded_function_name_origin_libary,
        self.description,self.parameters,self.output,self.additional_libraries,self.additional_resources,self.links)
        return self.picture

    def upload_data_to_sf(self):
        self.function_name = self.upload_data_to_sf.__name__
        self.origin_library = "utils_coverage.py"
        self.embeded_function_name = "NA"
        self.embeded_function_name_origin_libary = "NA"
        self.description = """
        This function generates a connection to Snowflake using your cisco e-mail and uploads a table from a Dataframe of serial numbers.
        """ 
        self.parameters = """
        1. df: DataFrame with information about serial numbers.
        2. user: a variable that stores your Cisco email to get authenticated.
        """ 
        self.output = "NA"
        self.picture = help_functions.load_image(self, f"{self.my_path}upload_data_to_sf.png")
        
        self.additional_libraries = """
        1. connector from Snowflake:
        2. write_pandas: 
        """
        self.additional_resources = "Please refer to following code to get additional information: 'help(parameter_name)'"
        self.links = """
        1. connector documentation: https://docs.snowflake.com/en/user-guide/python-connector-example.html
        2. write_pandas documentation: https://docs.snowflake.com/en/user-guide/python-connector-api.html#write_pandas
        """
        help_functions.my_print(self, self.function_name,self.origin_library,self.embeded_function_name,self.embeded_function_name_origin_libary,
        self.description,self.parameters,self.output,self.additional_libraries,self.additional_resources,self.links)
        return self.picture

class oe(cr,help_functions):
    
    def oe_flowchart(self):
        self.function_name = self.oe_flowchart.__name__
        self.origin_library = "help_lib.py"
        self.embeded_function_name = "NA"
        self.embeded_function_name_origin_libary = "NA"
        self.description = "You can use this flowchart to better review the current Estimator Reports creation process."
        self.parameters = "NA"
        self.output = "A picture with the flowchart for Estimator Report will be displayed with this function."
        self.picture = help_functions.load_image(self, f"{self.my_path}Estimator Flowchart.png")
        self.additional_libraries = "NA"
        self.additional_resources = "Please refer to following code to get addicional information: 'help(parameter_name)'"
        #self.links = "You can find the full size image at: " + self.my_path + "Coverage flowchart.png"
        self.links = "You can find the full size image at: " + (bcolors.UNDERLINE + self.my_path + "Estimator Flowchart.png" + bcolors.ENDC)
        help_functions.my_print(self, self.function_name,self.origin_library,self.embeded_function_name,self.embeded_function_name_origin_libary,
        self.description,self.parameters,self.output,self.additional_libraries,self.additional_resources,self.links)
        return self.picture

    def get_cav_names(self):
        
        self.function_name = self.get_cav_names.__name__
        self.origin_library = "utils_lib.py"
        self.embeded_function_name = "NA"
        self.embeded_function_name_origin_libary = "NA"
        self.description = """
        This function search and retrieve the CAV Name and CAV ID data from the snowflake database using the IDs (CAVs), of all accounts in process. 
        """
        self.parameters = """
        1. user: username or email used to connect with the snowflake database
        2. cavs: list of all CAVs ids got from the accounts in process
        """ 
        self.output = """returns a dataframe with the following columns:
        'CAV ID','CAV NAME'"""
        self.picture = help_functions.load_image(self,f"{self.my_path}get_cav_names.png") 
        self.additional_libraries = """
        1. Connector from snowflake.
        2. DataFrame from pandas.
        """
        self.additional_resources = "Please refer to following code to get additional information: 'help(parameter_name)'"
        self.links = """
        1. Using Python Connector for Snowflake: https://docs.snowflake.com/en/user-guide/python-connector-example.html
        """
        help_functions.my_print(self, self.function_name,self.origin_library,self.embeded_function_name,self.embeded_function_name_origin_libary,
        self.description,self.parameters,self.output,self.additional_libraries,self.additional_resources,self.links)
        return self.picture

    def get_telemetry_df2(self):
        
        self.function_name = self.get_telemetry_df2.__name__
        self.origin_library = "utils_lib.py"
        self.embeded_function_name = "NA"
        self.embeded_function_name_origin_libary = "NA"
        self.description = """
        This function search and retrieve the telemetry data from the snowflake database using the IDs (SAVs,GUs,Parties,CAVs), of all accounts in process. 
        """
        self.parameters = """
        1. user: username or email used to connect with the snowflake database
        2. savs: list of all SAVs ids got from the accounts in process
        3. gus: list of all GUs ids got from the accounts in process
        4. parties: list of all partie ids got from the accounts in process
        5. cavs: list of all CAVs ids got from the accounts in process
        """ 
        self.output = """returns a dataframe with the following columns:
        'Party ID','Customer', 'Equipment Type Description', 'Appliance ID','Inventory', 'Collection Date', 'Imported By', 'Product ID', 'Product Family', 
        'Business Entity', 'Sub Business Entity', 'Business Entity Description', 'PF', 'Product Description', 'Equipment Type', 'Product Type', 'Serial Number', 
        'Last Date of Support', 'Alert URL', 'Contract Number', 'Contract Status', 'Contract Lines Status', 'Service Program', 'Contract End Date', 
        'Contract Line End Date','ACCOUNT_ID','ID','Updated Date'"""
        self.picture = help_functions.load_image(self,f"{self.my_path}get_telemetry_df2.png") 
        self.additional_libraries = """
        1. Connector from snowflake.
        2. DataFrame from pandas.
        """
        self.additional_resources = "Please refer to following code to get additional information: 'help(parameter_name)'"
        self.links = """
        1. Using Python Connector for Snowflake: https://docs.snowflake.com/en/user-guide/python-connector-example.html
        """
        help_functions.my_print(self, self.function_name,self.origin_library,self.embeded_function_name,self.embeded_function_name_origin_libary,
        self.description,self.parameters,self.output,self.additional_libraries,self.additional_resources,self.links)
        return self.picture

    def get_tac_df_new(self):

        self.function_name = self.get_tac_df_new.__name__
        self.origin_library = "utils_lib.py"
        self.embeded_function_name = "NA"
        self.embeded_function_name_origin_libary = "NA"
        self.description = """
        This function retrieves information from Snowflake looking for an ID(s) and ID(s) Type related with service requests (SRs) of technical issues of the clients. 
        It includes SR's maximum severity, dates, resolution time, channel of receipt, incident status and other attributes. 
        """
        self.parameters = """
        1. user: a variable that stores your Cisco e-mail to get authenticated.
        2. ids: string with a ID list from the account(s) being processed.   
        3. id_type: string with a ID Type from the account(s) being processed. 
        """ 
        self.output = "A DataFrame with SRs data retrieved from a Snowflake table."
        self.picture = help_functions.load_image(self,f"{self.my_path}get_tac_df_new.png") 
        self.additional_libraries = """
        1. Connector from snowflake.
        2. DataFrame from pandas.  
        """
        self.additional_resources = "Please refer to following code to get additional information: 'help(parameter_name)'"
        self.links = """
        1. Using Python Connector for Snowflake: https://docs.snowflake.com/en/user-guide/python-connector-example.html
        """
        help_functions.my_print(self, self.function_name,self.origin_library,self.embeded_function_name,self.embeded_function_name_origin_libary,
        self.description,self.parameters,self.output,self.additional_libraries,self.additional_resources,self.links)
        return self.picture
        
    def set_datasource(self):

        self.function_name = self.set_datasource.__name__
        self.origin_library = "utils_lib.py"
        self.embeded_function_name = "NA"
        self.embeded_function_name_origin_libary = "NA"
        self.description = """
        This function creates the datasources (csv files) merging with the mapping files and formatting all columns according to each type of datasource (IB, Coverage, Subscription, Telemetry, and TAC). These files are read by Tableau.
        """
        self.parameters = """
        1. df: dataframe to be saved as csv file.
        2. type: define the file type and transformation type to be done over the dataframe.
        3. folder_path: directory where the csv file is going to be save.
        4. contract_types_list: contract type mapping.
        5. success_track_pricing_list: Success Tracks Pricing mapping.
        6. sspt_pricing_list_eligibleSSPT: Solution Support Eligibility mapping.
        7. sspt_pricing_list_outputTable: Solution Support Pricing mapping.
        8. sntc_pricing_list: SNTC Pricing mapping.
        9. htec_contract_types_htec: software mapping.
        10. htec_contract_types_swss:
        11. combined_services: combined services mapping.
        12. product_banding: product banding mapping.
        """ 
        self.output = "A DataFrame for each datasource with corresponding formatting to all columns."
        self.picture = help_functions.load_image(self,f"{self.my_path}set_datasource.png") 
        self.additional_libraries = "NA"
        self.additional_resources = "Please refer to following code to get additional information: 'help(parameter_name)'"
        self.links = "NA"
        help_functions.my_print(self, self.function_name,self.origin_library,self.embeded_function_name,self.embeded_function_name_origin_libary,
        self.description,self.parameters,self.output,self.additional_libraries,self.additional_resources,self.links)
        return self.picture

    def IB_attributes(self):

        self.function_name = self.IB_attributes.__name__
        self.origin_library = "utils_lib.py"
        self.embeded_function_name = "NA"
        self.embeded_function_name_origin_libary = "NA"
        self.description = """
        This function calculates IB Value, Percentage of Coverage and Major Renewal.
        """
        self.parameters = """
        1. IB: dataframe with IB data.
        2. Coverage: dataframe with Coverage data.
        """ 
        self.output = "A DataFrame with IB Value, Percentage of Coverage and Major Renewal."
        self.picture = help_functions.load_image(self,f"{self.my_path}IB_attributes.png") 
        self.additional_libraries = "NA"
        self.additional_resources = "Please refer to following code to get additional information: 'help(parameter_name)'"
        self.links = "NA"
        help_functions.my_print(self, self.function_name,self.origin_library,self.embeded_function_name,self.embeded_function_name_origin_libary,
        self.description,self.parameters,self.output,self.additional_libraries,self.additional_resources,self.links)
        return self.picture

    def ib_value_validation(self):

        self.function_name = self.ib_value_validation.__name__
        self.origin_library = "utils_lib.py"
        self.embeded_function_name = "NA"
        self.embeded_function_name_origin_libary = "NA"
        self.description = """
        This function verifies if the IB Value is less than cero.
        """
        self.parameters = """
        1. data: dataframe that contains in one of its columns the IB Value.
        """ 
        self.output = "A message 'Correct' if IB Value greater than cero, 'Incorrect' if less than cero or 'No IB data' otherwise."
        self.picture = help_functions.load_image(self,f"{self.my_path}IB_attributes.png") 
        self.additional_libraries = "NA"
        self.additional_resources = "Please refer to following code to get additional information: 'help(parameter_name)'"
        self.links = "NA"
        help_functions.my_print(self, self.function_name,self.origin_library,self.embeded_function_name,self.embeded_function_name_origin_libary,
        self.description,self.parameters,self.output,self.additional_libraries,self.additional_resources,self.links)
        return self.picture

    def ib_coverage_validation(self):

            self.function_name = self.ib_coverage_validation.__name__
            self.origin_library = "utils_lib.py"
            self.embeded_function_name = "NA"
            self.embeded_function_name_origin_libary = "NA"
            self.description = """
            This function verifies if the Coverage Percentage is in a range of 0 to 1.
            """
            self.parameters = """
            1. data: dataframe that contains in one of its columns the Coverage Percentage.
            """ 
            self.output = "A message 'Correct' if Coverage Percentage in range 0 to 1, 'Incorrect' if not or 'No IB data' otherwise."
            self.picture = help_functions.load_image(self,f"{self.my_path}ib_covered_validation.png") 
            self.additional_libraries = "NA"
            self.additional_resources = "Please refer to following code to get additional information: 'help(parameter_name)'"
            self.links = "NA"
            help_functions.my_print(self, self.function_name,self.origin_library,self.embeded_function_name,self.embeded_function_name_origin_libary,
            self.description,self.parameters,self.output,self.additional_libraries,self.additional_resources,self.links)
            return self.picture

    def rw_validation(self):

        self.function_name = self.rw_validation.__name__
        self.origin_library = "utils_lib.py"
        self.embeded_function_name = "NA"
        self.embeded_function_name_origin_libary = "NA"
        self.description = """
        This function verifies if the Major Renewal is empty or not.
        """
        self.parameters = """
        1. data: dataframe that contains in one of its columns the Major Renewal.
        """ 
        self.output = "A message 'Empty Value' if Major Renewal is empty, 'Correct' if it is not empty or 'No IB data' otherwise."
        self.picture = help_functions.load_image(self,f"{self.my_path}rw_validation.png") 
        self.additional_libraries = "NA"
        self.additional_resources = "Please refer to following code to get additional information: 'help(parameter_name)'"
        self.links = "NA"
        help_functions.my_print(self, self.function_name,self.origin_library,self.embeded_function_name,self.embeded_function_name_origin_libary,
        self.description,self.parameters,self.output,self.additional_libraries,self.additional_resources,self.links)
        return self.picture

    def oppty_validation(self):

        self.function_name = self.oppty_validation.__name__
        self.origin_library = "utils_lib.py"
        self.embeded_function_name = "NA"
        self.embeded_function_name_origin_libary = "NA"
        self.description = """
        This function verifies if the opportunity value on the argument is positive or negative. 
        """
        self.parameters = """
        1. oppty: Calculated oppty value which usually comes from another function.
        """ 
        self.output = "It returns 'Correct' if the oppry value is positive or zero, otherwise it returns 'Incorrect'."
        self.picture = help_functions.load_image(self,f"{self.my_path}oppty_validation.png") 
        self.additional_libraries = "NA"
        self.additional_resources = "Please refer to following code to get additional information: 'help(parameter_name)'"
        self.links = "NA"
        help_functions.my_print(self, self.function_name,self.origin_library,self.embeded_function_name,self.embeded_function_name_origin_libary,
        self.description,self.parameters,self.output,self.additional_libraries,self.additional_resources,self.links)
        return self.picture

    def length_validation(self):
        
        self.function_name = self.length_validation.__name__
        self.origin_library = "utils_lib.py"
        self.embeded_function_name = "NA"
        self.embeded_function_name_origin_libary = "NA"
        self.description = """
        This function verifies if the argument exceeds the length limit of tableau with which the value is visible. 
        """
        self.parameters = """
        1. number: Value to be measured.
        2. length: Tableau visible lengh limit. By default is equal to 8.  
        """ 
        self.output = "It returns 'Big value' if the value exceeds the length parameter, if it is empty it returns 'No Data' and if the value is between the range it returns 'Correct'."
        self.picture = help_functions.load_image(self,f"{self.my_path}length_validation.png") 
        self.additional_libraries = "NA"
        self.additional_resources = "Please refer to following code to get additional information: 'help(parameter_name)'"
        self.links = "NA"
        help_functions.my_print(self, self.function_name,self.origin_library,self.embeded_function_name,self.embeded_function_name_origin_libary,
        self.description,self.parameters,self.output,self.additional_libraries,self.additional_resources,self.links)
        return self.picture

    def smartsheet_len_info(self):

        self.function_name = self.smartsheet_len_info.__name__
        self.origin_library = "utils_lib.py"
        self.embeded_function_name = "NA"
        self.embeded_function_name_origin_libary = "NA"
        self.description = """
        This function verifies if the information returned from smartsheet that corresponds on the report to the section 'Package info', exceeds the length limit of tableau with which the value is visible. 
        """
        self.parameters = """
        1. df: Dataframe where is storage the data that is retrieved from the smartsheet regarding the requests.
        """ 
        self.output = "It returns 'QA Package Info' if the value exceeds the length limit, otherwise it returns 'Correct'."
        self.picture = help_functions.load_image(self,f"{self.my_path}smartsheet_len_info.png") 
        self.additional_libraries = "NA"
        self.additional_resources = "Please refer to following code to get additional information: 'help(parameter_name)'"
        self.links = "NA"
        help_functions.my_print(self, self.function_name,self.origin_library,self.embeded_function_name,self.embeded_function_name_origin_libary,
        self.description,self.parameters,self.output,self.additional_libraries,self.additional_resources,self.links)
        return self.picture        

    def SSPT_Oppty(self):

        self.function_name = self.SSPT_Oppty.__name__
        self.origin_library = "utils_lib.py"
        self.embeded_function_name = "LDoS_flag || select_data_to_display || service_level || uplift_recommended_sl"
        self.embeded_function_name_origin_libary = "NA"
        self.description = """
        This function calculates .
        """
        self.parameters = """
        1. data: dataframe that contains in one of its columns the Major Renewal.
        """ 
        self.output = "A message 'Empty Value' if Major Renewal is empty, 'Correct' if it is not empty or 'No IB data' otherwise."
        self.picture = help_functions.load_image(self,f"{self.my_path}rw_validation.png") 
        self.additional_libraries = "NA"
        self.additional_resources = "Please refer to following code to get additional information: 'help(parameter_name)'"
        self.links = "NA"
        help_functions.my_print(self, self.function_name,self.origin_library,self.embeded_function_name,self.embeded_function_name_origin_libary,
        self.description,self.parameters,self.output,self.additional_libraries,self.additional_resources,self.links)
        return self.picture


    def ST_Oppty(self):
        
        self.function_name = self.ST_Oppty.__name__
        self.origin_library = "utils_lib.py"
        self.embeded_function_name = "NA"
        self.embeded_function_name_origin_libary = "NA"
        self.description = """
        This function calculates the Success Tracks opportunity. 
        """
        self.parameters = """
        1. IB: dataframe that contains IB data.
        """ 
        self.output = "SNTC opportunity value in KUSD."
        self.picture = "NA"
        self.additional_libraries = "NA"
        self.additional_resources = "Please refer to following code to get additional information: 'help(parameter_name)'"
        self.links = "NA"
        help_functions.my_print(self, self.function_name,self.origin_library,self.embeded_function_name,self.embeded_function_name_origin_libary,
        self.description,self.parameters,self.output,self.additional_libraries,self.additional_resources,self.links)
        return self.picture

    def expert_care_verification(self):
        
        self.function_name = self.expert_care_verification.__name__
        self.origin_library = "utils_lib.py"
        self.embeded_function_name = "NA"
        self.embeded_function_name_origin_libary = "NA"
        self.description = """
        This function calculates  Expert Care oppty for verification used in conjunction with the oppty_validation function.
        """
        self.parameters = """
        1. expert_care: expert care mapping file .
        """ 
        self.output = "Expert Care opportunity value in KUSD."
        self.picture = help_functions.load_image(self,f"{self.my_path}expert_care.png") 
        self.additional_libraries = "NA"
        self.additional_resources = "Please refer to following code to get additional information: 'help(parameter_name)'"
        self.links = "NA"
        help_functions.my_print(self, self.function_name,self.origin_library,self.embeded_function_name,self.embeded_function_name_origin_libary,
        self.description,self.parameters,self.output,self.additional_libraries,self.additional_resources,self.links)
        return self.picture

    def smartnet_verification(self):
        self.function_name = self.smartnet_verification.__name__
        self.origin_library = "utils_lib.py"
        self.embeded_function_name = "NA"
        self.embeded_function_name_origin_libary = "NA"
        self.description = """
        This function calculates the smartnet value for verification used in conjunction with the oppty_validation function.
        The value verified come from a Installed Base column called 'Default Service List Price USD.'
        """
        self.parameters = """
        1. ib: Installed Base dataframe.
        """ 
        self.output = "Returns the sum of the Installed Base column 'Default Service List Price USD."
        self.picture = help_functions.load_image(self,f"{self.my_path}smartnet_verification.png") 
        self.additional_libraries = "NA"
        self.additional_resources = "Please refer to following code to get additional information: 'help(parameter_name)'"
        self.links = "NA"
        help_functions.my_print(self, self.function_name,self.origin_library,self.embeded_function_name,self.embeded_function_name_origin_libary,
        self.description,self.parameters,self.output,self.additional_libraries,self.additional_resources,self.links)
        return self.picture

    def smartnet_total_care_NBD_list_price(self):
        
        self.function_name = self.smartnet_verification.__name__
        self.origin_library = "utils_lib.py"
        self.embeded_function_name = "NA"
        self.embeded_function_name_origin_libary = "NA"
        self.description = """
        This function calculates the estimated value for SNTC (Installed Base column 'Annualized Extended Contract Line List USD Amount'). To do so, there are some calculations done like the prices
        of some Service Levels, filtering negative contract types values, null Product IDs, excluding a list of Contract Type, filtering by Product Banding (High and Mid), IB covered.
        """
        self.parameters = """
        1. ib: Installed Base dataframe.
        """ 
        self.output = "Returns the estimated value for SNTC (Installed Base column 'Annualized Extended Contract Line List USD Amount')"
        self.picture = help_functions.load_image(self,f"{self.my_path}smartnet_total_care_NBD_list_price.png") 
        self.additional_libraries = "NA"
        self.additional_resources = "Please refer to following code to get additional information: 'help(parameter_name)'"
        self.links = "NA"
        help_functions.my_print(self, self.function_name,self.origin_library,self.embeded_function_name,self.embeded_function_name_origin_libary,
        self.description,self.parameters,self.output,self.additional_libraries,self.additional_resources,self.links)
        return self.picture

    def estimated_list_price(self):
        
        self.function_name = self.estimated_list_price.__name__
        self.origin_library = "utils_lib.py"
        self.embeded_function_name = "NA"
        self.embeded_function_name_origin_libary = "NA"
        self.description = """
        This function calculates the opportunity for Success Tracks Level 2 (Installed Base column 'ST Estimated List Price'). To do so, there are some calculations done like the prices
        of some Service Levels, filtering negative contract types values, null Product IDs, excluding a list of Contract Type, filtering by Product Banding (High and Mid), IB covered.
        """
        self.parameters = """
        1. ib: Installed Base dataframe.
        """ 
        self.output = "Returns the estimated opportunity for Sucess Tracks (Installed Base column 'ST Estimated List Price')"
        self.picture = help_functions.load_image(self,f"{self.my_path}estimated_list_price.png") 
        self.additional_libraries = "NA"
        self.additional_resources = "Please refer to following code to get additional information: 'help(parameter_name)'"
        self.links = "NA"
        help_functions.my_print(self, self.function_name,self.origin_library,self.embeded_function_name,self.embeded_function_name_origin_libary,
        self.description,self.parameters,self.output,self.additional_libraries,self.additional_resources,self.links)
        return self.picture

    def fill_nas(self):
        
        self.function_name = self.fill_nas.__name__
        self.origin_library = "utils_lib.py"
        self.embeded_function_name = "NA"
        self.embeded_function_name_origin_libary = "NA"
        self.description = """
        Fills the blank spaces of the columns depending on the type of data they contain, if it is an int type record and it is empty it is filled with 0, if it is a float type it is filled with 0.0..
        """ 
        self.parameters = "df: It refers to any pandas DataFrame"
        self.output = "brings the df without blank spaces in the columns"
        self.picture = help_functions.load_image(self, f"{self.my_path}fill_nas.png")
        self.additional_libraries = "NA"
        self.additional_resources = "Please refer to following code to get additional information: 'help(parameter_name)'"
        self.links = "NA"
        help_functions.my_print(self, self.function_name,self.origin_library,self.embeded_function_name,self.embeded_function_name_origin_libary,
        self.description,self.parameters,self.output,self.additional_libraries,self.additional_resources,self.links)
        return self.picture
    

class error(help_functions):

    def df_not_defined(self):
        self.error_name="in python there is an error code that is called 'NameError' that is raised when a variable is not found in the local or global scope."
        self.error_image=help_functions.load_image(self, f"{self.my_path}df_not_defined.png")
        self.solution="Make shrure that you ran all the code section when the variable is created."
        self.solution_images=help_functions.load_image(self, f"{self.my_path}df_not_defined.png")
        self.founded_in = "The version of the templete in where was detected the error"
        help_functions.my_print_errors(self.error_name,self.solution,self.founded_in)
        return self.error_image
                                                       
    def https_connection_pool(self):
        self.error_name="HTTPSConnectionPool(host='cx-tableau-stage.cisco.com', port=443): Max retries exceeded with url: /api/3.13/serverinfo (Caused by NewConnectionError('<urllib3.connection.HTTPSConnection object at 0x000001E684E00250>: Failed to establish a new connection:             [WinError 10061] No se puede establecer una conexión ya que el equipo de destino denegó expresamente dicha conexión'))."
        self.error_image=help_functions.load_image(self, f"{self.my_path}HTTPSConnectionPool.png")
        self.solution="Due to a CX-Stage tableau error. It is necessary to notify as it is not a question of the code but of tableau permissions with CISCO."
        self.solution_images= ""
        self.founded_in="New folders 2.0.5.4"
        help_functions.my_print_errors(self.error_name,self.solution,self.founded_in)
        return self.error_image