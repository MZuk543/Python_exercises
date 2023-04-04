import ibm_db
import ibm_db_dbi
import json
# import pandas as pd
import sys
from typing import List, Optional, Tuple


def establish_dsn() -> Tuple[Optional[dict], Optional[str]]:
    """
        dsn_driver = '{IBM DB2 ODBC DRIVER}'
        dsn_database = ''  # e.g. 'BLUDB'
        dsn_hostname = ''  # e.g.: 'dashdb-txn-sbox-yp-dal09-04.services.dal.bluemix.net'
        dsn_port = ''  # e.g. '50000'
        dsn_protocol = ''  # i.e. 'TCPIP'
        dsn_uid = ''  # e.g. 'abc12345'
        dsn_pwd = ''  # e.g. '7dBZ3wWt9XN6$o0J'
        dsn_security = ''  # i.e. 'SSL'
    """
    
    # credentials
    """credentials = {'dsn_driver': '{IBM DB2 ODBC DRIVER}',
                   'dsn_database': 'BLUDB',
                   'dsn_hostname': '...',
                   'dsn_port': '...',
                   'dsn_protocol': 'TCPIP',
                   'dsn_uid': '...',
                   'dsn_pwd': '...',
                   'dsn_security': 'SSL'}"""
    credentials = read_credentials('db2_credentials.json')
    
    # Seting variable to create database connection
    if credentials:
        dsn = (
            'DRIVER={0};'
            'DATABASE={1};'
            'HOSTNAME={2};'
            'PORT={3};'
            'PROTOCOL={4};'
            'UID={5};'
            'PWD={6};'
            'SECURITY={7};').format(credentials['dsn_driver'], credentials['dsn_database'],
                                    credentials['dsn_hostname'], credentials['dsn_port'],
                                    credentials['dsn_protocol'], credentials['dsn_uid'],
                                    credentials['dsn_pwd'], credentials['dsn_security'])
        
        return credentials, dsn
    else:
        return None, None
    

def read_credentials(f_dir) -> Optional[dict]:
    try:
        with open(f_dir, "r") as file:
            data = json.load(file)
    except FileNotFoundError as e:
        print('Invalid credentials file.')
        return None
    else:
        credentials = {"dsn_driver": "{IBM DB2 ODBC DRIVER}",
                       "dsn_database": data["connection"]["db2"]["database"],
                       "dsn_hostname": data["connection"]["db2"]["hosts"][0]["hostname"],
                       "dsn_port": data["connection"]["db2"]["hosts"][0]["port"],
                       "dsn_protocol": "TCPIP",
                       "dsn_uid": data["connection"]["db2"]["authentication"]["username"],
                       "dsn_pwd": data["connection"]["db2"]["authentication"]["password"],
                       "dsn_security": "SSL"}
        
        return credentials
    
    
def connect_to_db() -> Optional[ibm_db_dbi.Connection]:
    credentials, dsn = establish_dsn()
    if credentials and dsn:
        try:
            conn = ibm_db.connect(dsn, "", "")
        except:
            print("Unable to connect: ", ibm_db.conn_errormsg())
            print()
            return None
            # !!!exit
        else:
            print("Connected to database: ", credentials["dsn_database"], "as user: ", credentials["dsn_uid"], "on host: ", credentials["dsn_hostname"])
            print()
            return conn
    else:
        print("Unable to get credentials and connect to data base")
        print()
        return None
    

def ibm_db2_cloud_simple_querying() -> bool:
    """
    Exercise to connect and query db2 Lite cloud database.
    Based on a jupyter notebook by IBM
    :return: True if succeeded, False otherwise
    """
    conn = connect_to_db()
    if conn:
        # Drop the table INSTRUCTOR in case it exists from a previous attempt
        dropQuery = "drop table INSTRUCTOR"
        dropStmt = ibm_db.exec_immediate(conn, dropQuery)

        # Construct the Create Table DDL statement
        createQuery = "create table INSTRUCTOR(id INTEGER PRIMARY KEY NOT NULL, fname VARCHAR(20)," \
                      "lname VARCHAR(20), city VARCHAR(20), ccode char(2))"
        createStmt = ibm_db.exec_immediate(conn, createQuery)

        # Inserting data
        insertQuery = "insert into INSTRUCTOR values (1, 'Rav', 'Ahuja', 'TORONTO', 'CA')," \
                      "(2, 'Raul', 'Chong', 'Markham', 'CA'), (3, 'Hima', 'Vasudevan', 'Chicago', 'US')"
        insertStmt = ibm_db.exec_immediate(conn, insertQuery)
        
        # Selecting data
        selectQuery = "select * from INSTRUCTOR"
        selectStmt = ibm_db.exec_immediate(conn, selectQuery)
        # Fetch the Dictionary (for the first row only)
        ibm_db.fetch_both(selectStmt)

        # Fetch the rest of the rows and print the ID and FNAME for those rows
        while ibm_db.fetch_row(selectStmt) != False:
            print(" ID:", ibm_db.result(selectStmt, 0), " FNAME:", ibm_db.result(selectStmt, "FNAME"))

        ibm_db.close(conn)
        return True
    else:
        print("Unable to query database due to no connection object")
        return False


def answer(conn: ibm_db_dbi.Connection, question: str, columns: List[str], sql_query: str):
    GREEN_COLOR = '\033[92m'
    END_COLOR = '\033[0m'
    
    columns_number = len(columns)
    
    result = ibm_db.exec_immediate(conn, sql_query)
    # df = pd.read_sql(sql_query, conn)
    dl = []
    col_lengths = []
    data_record = ibm_db.fetch_both(result)
    while data_record:
        dd = []
        cl = []
        for _ in range(columns_number):
            dd.append(data_record[columns[_]])
            cl.append(len(str(data_record[columns[_]])))
        dl.append(dd)
        col_lengths.append(cl)
        data_record = ibm_db.fetch_both(result)

    columns_print_str = ''
    line_print_str = ''
    
    # Finding the max length of the column for each column, to pretty print
    max_col_length = []
    for i in range(columns_number):
        lens_in_col = []
        for j in range(len(col_lengths)):
            lens_in_col.append(col_lengths[j][i])
        m = max(lens_in_col)
        if len(columns[i]) > m:
            m = len(columns[i])
        max_col_length.append(m)
        
    for i in range(columns_number):
        n = max_col_length[i] + 3
        columns_print_str += f'{{:{n}}} '
        for j in range(n):
            line_print_str += '-'
        line_print_str += ' '
    columns_print_str = columns_print_str.rstrip()
    
    print()
    print(question)
    print(GREEN_COLOR + sql_query + END_COLOR)
    print(columns_print_str.format(*columns))
    print(line_print_str)
    for _ in range(len(dl)):
        print(columns_print_str.format(*dl[_]))


def ibm_db2_final_assign() -> bool:
    """
    Exercise to query Chicago Census Data, Chicago Public Schools, Chicago Crime Data.
    Tables are to be created earlier.
    :return: True if succeeded, False otherwise
    """
    conn = connect_to_db()
    if conn:
        columns: List[str] = []
        
        print()
        print('------------------ FINAL ASSIGNMENT ANSWERS: ------------------')
        print()
        
        question = '1. Find the total number of crimes recorded in the CRIME table.'
        columns.clear()
        columns.append('COUNT_CRIMES')
        sql = "select count('CASE_NUMBER') as COUNT_CRIMES from CHICAGO_CRIME_DATA"
        answer(conn, question, columns, sql)
        
        question = '2. List community areas with per capita income less than 11000.'
        columns.clear()
        columns.append('COMMUNITY_AREA_NAME')
        columns.append('PER_CAPITA_INCOME')
        sql = 'select COMMUNITY_AREA_NAME, PER_CAPITA_INCOME from census_data where PER_CAPITA_INCOME < 11000'
        answer(conn, question, columns, sql)
        
        question = '3. List all case numbers for crimes involving minors.'
        columns.clear()
        columns.append('CASE_NUMBER')
        sql = "select CASE_NUMBER from chicago_crime_data where lcase(DESCRIPTION) like '%minor%'"
        answer(conn, question, columns, sql)
        
        question = '4. List all kidnapping crimes involving a child.'
        columns.clear()
        columns.append('CASE_NUMBER')
        columns.append('PRIMARY_TYPE')
        columns.append('DESCRIPTION')
        sql = "select CASE_NUMBER, PRIMARY_TYPE, DESCRIPTION from chicago_crime_data " \
               "where lcase(PRIMARY_TYPE) = 'kidnapping' and lcase(DESCRIPTION) like '%child%'"
        answer(conn, question, columns, sql)

        question = '5. What kind of crimes were recorded at schools?'
        columns.clear()
        columns.append('PRIMARY_TYPE')
        columns.append('DESCRIPTION')
        columns.append('LOCATION_DESCRIPTION')
        sql = "select PRIMARY_TYPE, DESCRIPTION, LOCATION_DESCRIPTION from chicago_crime_data " \
               "where lcase(LOCATION_DESCRIPTION) like '%school%'"
        answer(conn, question, columns, sql)

        question = '6. List the average safety score for each type of school.'
        columns.clear()
        columns.append('ELEMENTARY__MIDDLE__OR_HIGH_SCHOOL')
        columns.append('AVG_SAFETY_SCORE')
        sql = 'select ELEMENTARY__MIDDLE__OR_HIGH_SCHOOL, avg(SAFETY_SCORE) as AVG_SAFETY_SCORE from ' \
              'chicago_public_schools group by ELEMENTARY__MIDDLE__OR_HIGH_SCHOOL'
        answer(conn, question, columns, sql)

        question = '7. List 5 community areas with the highest % of households below poverty line.'
        columns.clear()
        columns.append('COMMUNITY_AREA_NAME')
        columns.append('PERCENT_HOUSEHOLDS_BELOW_POVERTY')
        sql = 'select COMMUNITY_AREA_NAME, PERCENT_HOUSEHOLDS_BELOW_POVERTY from ' \
              'census_data order by PERCENT_HOUSEHOLDS_BELOW_POVERTY desc limit 5'
        answer(conn, question, columns, sql)

        question = '8. Which community area is most crime prone?'
        columns.clear()
        columns.append('COMMUNITY_AREA_NUMBER')
        columns.append('CRIMES_TOTAL')
        sql = 'select COMMUNITY_AREA_NUMBER, count(ID) as CRIMES_TOTAL from ' \
              'chicago_crime_data group by COMMUNITY_AREA_NUMBER order by CRIMES_TOTAL desc limit 1'
        answer(conn, question, columns, sql)

        question = '9. Use a sub-query to find the name of the community area with the highest hardship index.'
        columns.clear()
        columns.append('COMMUNITY_AREA_NAME')
        sql = 'select COMMUNITY_AREA_NAME from ' \
              '(select HARDSHIP_INDEX, COMMUNITY_AREA_NAME from ' \
              'census_data order by HARDSHIP_INDEX desc nulls last limit 1)'
        answer(conn, question, columns, sql)

        question = '10. Use a sub-query to determine the community area name with most number of crimes.'
        columns.clear()
        columns.append('COMMUNITY_AREA_NAME')
        columns.append('CRIMES_TOTAL')
        sql = 'select CD.COMMUNITY_AREA_NAME, CC.CRIMES_TOTAL from census_data as CD, ' \
              '(select COMMUNITY_AREA_NUMBER, count(ID) as CRIMES_TOTAL from chicago_crime_data ' \
              'group by COMMUNITY_AREA_NUMBER order by CRIMES_TOTAL desc nulls last limit 1) as CC ' \
              'where CD.COMMUNITY_AREA_NUMBER = CC.COMMUNITY_AREA_NUMBER'
        answer(conn, question, columns, sql)

        question = '11. List all crimes that took place at a school. Include case number, crime type and community name.'
        columns.clear()
        columns.append('CASE_NUMBER')
        columns.append('PRIMARY_TYPE')
        columns.append('COMMUNITY_AREA_NAME')
        sql = "select C.CASE_NUMBER, C.PRIMARY_TYPE, S.COMMUNITY_AREA_NAME from CHICAGO_CRIME_DATA as C, " \
              "left outer join CHICAGO_PUBLIC_SCHOOLS as S" \
              "on C.COMMUNITY_AREA_NUMBER = S.COMMUNITY_AREA_NUMBER" \
              "where C.LOCATION_DESCRIPTION like '%school%' or C.LOCATION_DESCRIPTION like '%School%'" \
              "or C.LOCATION_DESCRIPTION like '%SCHOOL%'."
        answer(conn, question, columns, sql)

        
        ibm_db.close(conn)
        return True
    else:
        print("Unable to query database due to no connection object")
        return False


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    # ibm_db2_cloud_simple_querying()
    sys.exit(ibm_db2_final_assign())

