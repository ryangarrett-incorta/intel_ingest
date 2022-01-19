# synapse Load - V1.3.8.5.2.1
# Hotfix - remove MV validation during script updates and sql credential masking in log output
import logging
import pyodbc
import psycopg2
import pandas as pd
import time
import csv
from datetime import datetime
import os, uuid, sys
from azure.storage.filedatalake import DataLakeServiceClient
from pyspark.sql import SparkSession
import pyarrow.parquet as pq
import incorta_pyarrowfs_adlgen2 as pyfs_adls
from azure.identity import ClientSecretCredential
from csv_logging import csvlogger
import uuid
from pyspark.sql.types import *
import re

spark = SparkSession.builder.getOrCreate()

# ADLS properties
adls_auth_method = spark.conf.get("spark.ADLSConnection.authenticationmethod")
adls_client_secret = spark.conf.get("spark.ADLSConnection.clientsecret")
adls_account_name = spark.conf.get("spark.ADLSConnection.accountname")
adls_file_system = spark.conf.get("spark.ADLSConnection.filesystem")
adls_root_dir = spark.conf.get("spark.ADLSConnection.rootdirectory")

# Active Directory App properties
aad_tenant_id = spark.conf.get("spark.ADLSConnection.adsp_tenant_id")
aad_client_id = spark.conf.get("spark.ADLSConnection.adsp_client_id")
aad_client_secret = spark.conf.get("spark.ADLSConnection.adsp_client_secret")
aad_sp_identity = aad_client_id + "@https://login.microsoftonline.com/" + aad_tenant_id + "/oauth2/token"

# Incorta properties
incorta_host = spark.conf.get("spark.IncortaConnectionString.host")
incorta_db_name = spark.conf.get("spark.IncortaConnectionString.dbname")
incorta_user = spark.conf.get("spark.IncortaConnectionString.user")
incorta_password = spark.conf.get("spark.IncortaConnectionString.password")
incorta_port = spark.conf.get("spark.IncortaConnectionString.port")

IncortaConnectionString = "host={} dbname={} user={} password={} port={}".format(incorta_host, incorta_db_name,
                                                                                 incorta_user, incorta_password,
                                                                                 incorta_port)

# Synapse properties
loaderuser = spark.conf.get("spark.SynapseConnectionString.loaderuser")
synapse_driver = spark.conf.get("spark.SynapseConnectionString.driver")
synapse_server = spark.conf.get("spark.SynapseConnectionString.server")
synapse_database = spark.conf.get("spark.SynapseConnectionString.database")
synapse_uid = spark.conf.get("spark.SynapseConnectionString.uid")
synapse_password = spark.conf.get("spark.SynapseConnectionString.pwd")
synapse_mars_connection_enabled = spark.conf.get("spark.SynapseConnectionString.mars.connection.enabled")
synapse_application_name = spark.conf.get("spark.SynapseConnectionString.application.name")
synapse_port = spark.conf.get("spark.SynapseConnectionString.port")
synapse_external_file_format = spark.conf.get("spark.SynapseConnectionString.externalfileformat")
synapse_target_schema_name = spark.conf.get("spark.SynapseConnectionString.targetschemaname")  # V1.4 - if not specified use incorta schema name - MV local setting
synapse_target_table_name = spark.conf.get("spark.SynapseConnectionString.targettablename")  # V1.4 - if not specified use incorta table name - MV local setting
# synapse_table_name_delimiter = spark.conf.get("spark.SynapseConnectionString.delimitertext") #V1.4 - if not specified and using custom schema name, use default _Z_I_ - global setting

SynapseConnectionString = "DRIVER={};SERVER={},{};DATABASE={};UID={};PWD={};MARS_Connection={};Application Name={}".format(
    synapse_driver, synapse_server, synapse_port, synapse_database, synapse_uid, synapse_password,
    synapse_mars_connection_enabled, synapse_application_name)

synapse_mappings = {}
synapse_mappings_file_path = spark.conf.get("spark.IncortaSynapse.mappings_csv")

# file output level is set to ERROR, change it to INFO or DEBUG as per your requirement
for handler in logging.root.handlers[:]:
    logging.root.removeHandler(handler)

logging.basicConfig(format='%(asctime)s | %(levelname)s : %(message)s',
                    level=logging.INFO, stream=sys.stdout)

logging.getLogger('azure').setLevel(logging.ERROR)

def connect_to_incorta(schemaname, tablename=None):
    conn = None
    inc_conn_string = IncortaConnectionString

    sql = 'SELECT column_name, data_type, is_nullable'
    if tablename is None:
        sql += ', table_name'
    sql += " FROM information_schema.columns WHERE table_schema = '" + schemaname + "'"
    if tablename is not None:
        sql += " AND table_name = '" + tablename + "'"

    sql += " AND table_name NOT LIKE 'Load_Synapse%'"
    try:

        conn = psycopg2.connect(inc_conn_string)
        # create a cursor
        cur = conn.cursor()
        cur.execute(sql)

        # retrieve columns
        db_columns = cur.fetchall()
        ret_val = {}
        if tablename is not None:
            key_val = schemaname + "." + tablename
            ret_val[key_val] = db_columns
        else:
            for elem in db_columns:
                key = schemaname + "." + elem[-1]
                if key not in ret_val:
                    ret_val[key] = list()
                ret_val[key].append(elem[:-1])

        # close the communication with the PostgreSQL
        cur.close()
        listToStr = ' '.join([str(elem) for elem in db_columns])
        #logging.info('Established connection with Incorta and retreived columns - ' + listToStr)
    except (Exception, psycopg2.DatabaseError) as error:
        logging.critical('Exception while fetching results from Incorta over pgsql - ' + str(error))
        return error
    finally:
        if conn is not None:
            conn.close()
            # print('Database connection closed.')
    return ret_val


def create_PK_index_statement(pkindex, tablename, schemaname, loadtype, upsert=False, dedupe=False):
    # Create new PK constraint for each table
    sql_statement = []
    if loadtype == 'Full':
        sql_alter_statement = "ALTER TABLE " + schemaname + "." + tablename
        sql_constraint = " ADD  CONSTRAINT " + "PK_" + tablename + " PRIMARY KEY NONCLUSTERED ("

        sql_statement.append(sql_alter_statement)
        sql_statement.append(sql_constraint)
        i = 1
        for index, item in pkindex.iteritems():
            pk_column = item + " ASC"
            sql_statement.append(pk_column)
            if i < pkindex.size:
                sql_statement.append(',')
                i = i + 1
        sql_statement.append(") NOT ENFORCED;")

    elif loadtype == 'Incremental':
        if upsert:
            if dedupe:
                sql_constraint = ") ,CLUSTERED INDEX ("
                sql_statement.append(sql_constraint)
                i = 1
                for index, item in pkindex.iteritems():
                    pk_column = item + " ASC"
                    sql_statement.append(pk_column)
                    if i < pkindex.size:
                        sql_statement.append(',')
                        i = i + 1
                sql_close = ")) "
                sql_statement.append(sql_close)
            else:
                i = 1
                logging.info("Evaluating Merge statement condition")
                for index, item in pkindex.iteritems():
                    pk_column = "t1." + item + "= t2." + item
                    sql_statement.append(pk_column)
                    if i < pkindex.size:
                        sql_statement.append(' AND ')
                        i = i + 1
        else:
            # TODO: sql_statement is empty here, is this valid?

            sql_constraint = ") ,CLUSTERED INDEX ("
            sql_statement.append(sql_constraint)
            i = 1
            for index, item in pkindex.iteritems():
                pk_column = item + " ASC"
                sql_statement.append(pk_column)
                if i < pkindex.size:
                    sql_statement.append(',')
                    i = i + 1
            sql_selectas = "))  AS (SELECT * FROM " + schemaname + "." + tablename + " WHERE 1=2); "
            sql_statement.append(sql_selectas)


    else:
        logging.info('Missing Load Type')

    sql_string = ''.join([str(elem) for elem in sql_statement])
    return sql_string


def create_table_statement(sql_dict, tablename, schemaname, loadtype, upsert=False):
    # Create new table in synapse, if it doesnt already exist, based on the incorta table structure
    logging.info('Create main table statement retrieval execution begins')
    sql_statement = []
    # V 1.3.8.4 - removing unnecessary drop table since we already renaming this table later
    sql_create_table = "IF OBJECT_ID('" + schemaname + "." + tablename + "_new', 'U') IS NOT NULL DROP TABLE " + schemaname + "." + tablename + "_new; CREATE TABLE " + schemaname + "." + tablename + "_new"

    sql_create_table_inc = "IF OBJECT_ID('" + schemaname + ".inc_" + tablename + "_new', 'U') IS NOT NULL DROP TABLE " + schemaname + ".inc_" + tablename + "_new; CREATE TABLE " + schemaname + ".inc_" + tablename + "_new"
    pkindex = sql_dict.query('is_key_column=="YES"')['column_name']
    nopkhashkey = sql_dict.query('is_key_column=="NO"')['column_name']
    sql_select_as = " AS SELECT * FROM "

    #sql_rename_drop_table = "RENAME OBJECT " + schemaname + "." + tablename + " TO "  + tablename + "_old; RENAME OBJECT " + schemaname + "." + tablename + "_new" + " TO " + tablename + "; DROP TABLE " + schemaname + "." +  tablename + "_old; DROP TABLE " + schemaname + "." +  tablename + "_stg;"

    if loadtype == 'Full':
        
        sql_statement.append(sql_create_table)
        sql_index_inc = ''
        if not pkindex.empty: # remove distribtion for tables without keys
            sql_index_inc = " WITH ( DISTRIBUTION = HASH("
            sql_statement.append(sql_index_inc)
            # multiple columns are not currently supported in the distribution key - all tables default to HASH distribution
            try:
                hash_column = get_hash_column(schemaname, tablename)
                if hash_column is None:
                    hash_column = str(next(iter(pkindex)))
                else:
                    logging.info("Using user mapped hash column: " + hash_column)
                sql_statement.append(hash_column + '))')
                #sql_statement.append(str(next(iter(pkindex))) + '))')
            except Exception as error:
                logging.info("No keys present in Incorta tables")
        else:
            sql_index_inc = " WITH ( DISTRIBUTION = HASH("
            sql_statement.append(sql_index_inc)
            sql_statement.append(str(next(iter(nopkhashkey))) + '))')
        sql_statement.append(sql_select_as + schemaname + "." + tablename + "_stg;")
        #sql_statement.append(sql_rename_drop_table)
        

    elif loadtype == 'Incremental':
        # TODO: Should sql_create_schema be appended here?
        if upsert:
            sql_drop_tbl = """IF OBJECT_ID('{inc_stg_tbl}', 'U') IS NOT NULL DROP TABLE {inc_stg_tbl};""".format(
                inc_stg_tbl=schemaname + ".inc_" + tablename + "_new")

            # Create incremental staging table for de-duplication
            sql_temp_stg_tbl_create = """CREATE TABLE {inc_stg_tbl} """.format(
                inc_stg_tbl=schemaname + ".inc_" + tablename + "_new")

            sql_statement.append(sql_drop_tbl)
            sql_statement.append(sql_temp_stg_tbl_create)
            
            sql_index_inc = " WITH ( DISTRIBUTION = HASH("
            sql_statement.append(sql_index_inc)
            # multiple columns are not currently supported in the distribution key
            hash_column = get_hash_column(schemaname, tablename)
            if hash_column is None:
                hash_column = str(next(iter(pkindex)))
            else:
                logging.info("Using user mapped hash column: " + hash_column)

            sql_statement.append(hash_column)
            #sql_statement.append(str(next(iter(pkindex))))
            sql_index_end_inc = create_PK_index_statement(pkindex, tablename, schemaname, loadtype, True, dedupe=True)
            sql_statement.append(sql_index_end_inc)
            sql_ctas = """ AS (SELECT * FROM {inc_tbl} as t1 WHERE t1.LAST_UPDATE_DATE=
           (SELECT MAX(LAST_UPDATE_DATE) FROM {inc_tbl} t2 """.format(inc_tbl=schemaname + ".inc_" + tablename + "_stg",
                                                                      inc_stg_tbl=schemaname + ".inc_" + tablename + "_new")
            sql_temp_stg_tbl_where = """WHERE """
            sql_where_filter = create_PK_index_statement(pkindex, tablename, schemaname, loadtype, True, dedupe=False)
            sql_end = """));"""
            sql_statement.append(sql_ctas)

            sql_statement.append(sql_temp_stg_tbl_where)
            sql_statement.append(sql_where_filter)
            sql_statement.append(sql_end)

        else:
            
            sql_statement.append(sql_create_table_inc)
            if not pkindex.empty:
                sql_index_inc = " WITH ( DISTRIBUTION = HASH("
                sql_statement.append(sql_index_inc)

                # multiple columns are not currently supported in the distribution key
                #sql_statement.append(str(next(iter(pkindex))))
                hash_column = get_hash_column(schemaname, tablename)
                if hash_column is None:
                    hash_column = str(next(iter(pkindex)))
                else:
                    logging.info("Using user mapped hash column: " + hash_column)

                sql_statement.append(hash_column)
                sql_index_end_inc = create_PK_index_statement(pkindex, tablename, schemaname, loadtype, False, dedupe=False)
                sql_statement.append(sql_index_end_inc)
            else:
                sql_index_inc = " WITH ( DISTRIBUTION = HASH("
                sql_statement.append(sql_index_inc)
                sql_statement.append(str(next(iter(nopkhashkey))) + '))')
            sql_statement.append(sql_select_as + schemaname + "." + tablename + "_stg;")
            #sql_statement.append(sql_rename_drop_table)
    else:
        logging.info('Missing Load Type')

    sql_string = ''.join([str(elem) for elem in sql_statement])
    #logging.info('SQL string for table creation is - ' + sql_string)
    return sql_string


def create_stg_load_table_statement(sql_dict, tablename, schemaname, loadtype):
    # Create new table in synapse, if it doesnt already exist, based on the incorta table structure
    logging.info('Create stg table statement retrieval execution begins')
    sql_statement = []
    sql_create_schema = "IF NOT EXISTS ( SELECT  * FROM sys.schemas WHERE   name = N'" + schemaname + "' ) EXEC('CREATE SCHEMA [" + schemaname + "]');"

    # V1.3.8.4 - create a stg table if it doesnt exist
    sql_create_table = "IF OBJECT_ID('" + schemaname + "." + tablename + "_stg', 'U') IS NOT NULL DROP TABLE "  + schemaname + "." + tablename + "_stg;" + "CREATE TABLE " + schemaname + "." + tablename + "_stg ("
    sql_create_table_inc = "IF OBJECT_ID('" + schemaname + ".inc_" + tablename + "_stg', 'U') IS NOT NULL DROP TABLE " + schemaname + ".inc_" + tablename + "_stg;" + "CREATE TABLE " + schemaname + ".inc_" + tablename + "_stg ("

    # V1.3.8.2 - creating a stg table
    #sql_create_table = "IF OBJECT_ID('" + schemaname + "." + tablename + "_stg', 'U') IS NOT NULL DROP TABLE " + schemaname + "." + tablename + "_stg; CREATE TABLE " + schemaname + "." + tablename + "_stg ("

    #sql_create_table_inc = "IF OBJECT_ID('" + schemaname + ".inc_" + tablename + "_stg', 'U') IS NOT NULL DROP TABLE " + schemaname + ".inc_" + tablename + "_stg; CREATE TABLE " + schemaname + ".inc_" + tablename + "_stg ("
    
    pkindex = sql_dict.query('is_key_column=="YES"')['column_name']

    if loadtype == 'Full':
        sql_statement.append(sql_create_schema)
        sql_statement.append(sql_create_table)
        i = 1
        for index, row in sql_dict.iterrows():
            sql_columns = row['column_name'] + " " + row['mssql_dtype']
            sql_statement.append(sql_columns)
            if (row['is_nullable'] == 'YES'):
                sql_statement.append(' NULL')
            else:
                sql_statement.append(' NOT NULL')
            if i < len(sql_dict.index):
                sql_statement.append(',')
                i = i + 1
        sql_statement.append(');')
        
    elif loadtype == 'Incremental':
        sql_statement.append(sql_create_table_inc)
        i = 1
        for index, row in sql_dict.iterrows():
            sql_columns = row['column_name'] + " " + row['mssql_dtype']
            sql_statement.append(sql_columns)
            if (row['is_nullable'] == 'YES'):
                sql_statement.append(' NULL')
            else:
                sql_statement.append(' NOT NULL')
            if i < len(sql_dict.index):
                sql_statement.append(',')
                i = i + 1
        sql_statement.append(');')
            
    else:
        logging.info('Missing Load Type')

    sql_string = ''.join([str(elem) for elem in sql_statement])
    #logging.info('SQL string for table creation is - ' + sql_string)
    return sql_string


def create_copy_statement(sql_dict, tablename, schemaname, synapse_table_name, synapse_schema_name, loadtype,
                          inc_timestamp):
    logging.info('Create copy statement retrieval begins')
    sql_statement = ['']
    sql_file_loc = ['']
    sql_grant_insert_access =""
    secret = ""
    adls_loc = "{}://{}.dfs.core.windows.net/{}/{}/{}/".format(
        "https", adls_account_name, adls_file_system, adls_root_dir, schemaname)

    sql_execute_as = """EXECUTE AS USER='""" + loaderuser + """';"""
    sql_copy = "COPY INTO "
    if loadtype == 'Full':
        sql_grant_insert_access = "GRANT INSERT ON " + synapse_schema_name + "." + synapse_table_name + "_stg TO " + loaderuser + ";"  # V1.4
        sql_tbl = synapse_schema_name + "." + synapse_table_name + "_stg ("  # V1.4
        sql_from = """ ) FROM '""" + adls_loc + tablename + """'"""
    elif loadtype == 'Incremental':
        if inc_timestamp:
            sql_grant_insert_access = "GRANT INSERT ON " + synapse_schema_name + ".inc_" + synapse_table_name + "_stg TO " + loaderuser + ";"  # V1.4
            sql_tbl = synapse_schema_name + ".inc_" + synapse_table_name + "_stg ("  # V1.4
            sql_from = """ ) FROM '"""
            i = 1
            for ts in inc_timestamp:
                sql_file_loc.append(adls_loc + tablename + "." + str(ts) + """/*'""")
                if i < len(inc_timestamp):
                    sql_file_loc.append(""",'""")
                    i = i + 1
            sql_file_loc_string = ''.join([str(elem) for elem in sql_file_loc])
        else:
            # sql_from = """ FROM '""" + adls_loc + tablename + """/*'"""
            logging.info("No incremental folder was found. Abort load...")
    else:
        logging.info('Missing Load Type')
    sql_with = ""
    if adls_auth_method == 'serviceprincipal':
        secret = aad_client_secret
        sql_with = """ WITH ( FILE_FORMAT = """ + synapse_external_file_format + """, CREDENTIAL=(IDENTITY='""" + aad_sp_identity + """', SECRET='""" + secret + """'))"""
    else:
        secret = adls_client_secret
        sql_with = """ WITH ( FILE_FORMAT = """ + synapse_external_file_format + """, CREDENTIAL=(IDENTITY='Storage Account Key', SECRET='""" + secret + """'))"""

    sql_statement.append(sql_grant_insert_access)
    sql_statement.append(sql_execute_as)
    sql_statement.append(sql_copy)
    sql_statement.append(sql_tbl)
    j = 1
    for index, row in sql_dict.iterrows():
        if (row['is_nullable'] == 'YES'):
            sql_statement.append(row['column_name'])
        else:
            sql_statement.append(row['column_name'])
            if (row['mssql_dtype'] == '[int]' or row['mssql_dtype'] == '[bigint]' or row['mssql_dtype'] == '[float]'):
                sql_statement.append(' DEFAULT ' + row['default'])
            else:
                sql_statement.append(""" DEFAULT '""" + row['default'] + """'""")
        if j < len(sql_dict.index):
            sql_statement.append(',')
            j = j + 1

    sql_statement.append(sql_from)
    if inc_timestamp:
        sql_statement.append(sql_file_loc_string)
    sql_statement.append(sql_with)
    sql_statement.append(";")

    sql_string = ''.join([str(elem) for elem in sql_statement])
    #logging.info('SQL string for data load is ' + sql_string)
    return sql_string


def rename_tbl_statement(schemaname,tablename):
    sql_rename_table = "IF OBJECT_ID('" + schemaname + "." + tablename + "', 'U') IS NOT NULL " + "RENAME OBJECT " + schemaname + "." + tablename + " TO "  + tablename + "_old; RENAME OBJECT " + schemaname + "." + tablename + "_new" + " TO " + tablename + ";"
    #sql_string = ''.join([str(elem) for elem in sql_statement])
    #logging.info('SQL string for table creation is - ' + sql_string)
    return sql_rename_table


def delete_tbl_statement(schemaname,tablename):
    sql_statement = []
    sql_drop_old_tbl = "IF OBJECT_ID('" + schemaname + "." + tablename + "_old', 'U') IS NOT NULL DROP TABLE " + schemaname + "." +  tablename + "_old;"
    sql_statement.append(sql_drop_old_tbl)
    sqL_drop_stg_tbl = "IF OBJECT_ID('" + schemaname + "." + tablename + "_stg', 'U') IS NOT NULL DROP TABLE " + schemaname + "." +  tablename + "_stg;"
    sql_drop_new_tbl = "IF OBJECT_ID('" + schemaname + "." + tablename + "_new', 'U') IS NOT NULL DROP TABLE " + schemaname + "." +  tablename + "_new;"
    sql_statement.append(sqL_drop_stg_tbl)
    sql_statement.append(sql_drop_new_tbl)
    sql_string = ''.join([str(elem) for elem in sql_statement])
    #logging.info('SQL string for table creation is - ' + sql_string)
    return sql_string


def build_synapse_mappings():
    with open(synapse_mappings_file_path, newline="") as csv_file:
        reader = csv.DictReader(csv_file)
        for row in reader:
            set_hash_column(row["schema_name"], row["table_name"], row["hash_column"])


def get_hash_column(schema_name, table_name):
    try:
        return synapse_mappings["{}.{}".format(schema_name, table_name)]
    except:
        return None


def set_hash_column(schema_name, table_name, hash_column):
    synapse_mappings["{}.{}".format(schema_name, table_name)] = hash_column


def query_synapse(sql, is_fetchrow):
    status = " "
    syn_conn_string = SynapseConnectionString
    try:
        cnxn = pyodbc.connect(syn_conn_string, autocommit=True)
        cursor = cnxn.cursor()
        try:
            if is_fetchrow:
                cursor.execute(sql)
                row = cursor.fetchone()
                if row:
                    status = row[0]
                    logging.info("Synapse query execution returned - " + str(status))
            else:
                status = cursor.execute(sql).rowcount
                logging.info("Rows affected - " + str(status))
        except pyodbc.DatabaseError as err:
            cnxn.rollback()
            logging.info("Error during query execution - " + str(err))
            return err
        cnxn.close()

        return status
    except Exception as error:
        logging.critical('Exception while executing query on Synapse - ' + str(error))
        return error


def update_timestamp_statement(max_timestamp, synapse_table_name, synapse_schema_name, loadtype):
    sql_statement = []
    sql_create_schema = """IF NOT EXISTS ( SELECT  * FROM sys.schemas WHERE   name = N'INCORTA_INCREMENTAL' ) EXEC('CREATE SCHEMA [INCORTA_INCREMENTAL]');"""

    sql_create_table = """IF OBJECT_ID('INCORTA_INCREMENTAL.TableLastLoadTimestamp', 'U') IS NULL CREATE TABLE INCORTA_INCREMENTAL.TableLastLoadTimestamp(
       TABLE_NAME varchar(4000) NOT NULL,
   LOAD_TYPE varchar(4000) NOT NULL,
   FILE_EPOCH bigint NULL);"""

    sql_update_table = """IF EXISTS (SELECT * FROM INCORTA_INCREMENTAL.TableLastLoadTimestamp WHERE TABLE_NAME = '""" + synapse_schema_name + "." + synapse_table_name + """')
       BEGIN
           UPDATE INCORTA_INCREMENTAL.TableLastLoadTimestamp SET FILE_EPOCH =""" + max_timestamp + """,
           LOAD_TYPE = '""" + loadtype + """'
           WHERE TABLE_NAME = '""" + synapse_schema_name + "." + synapse_table_name + """'
       END
       ELSE
       BEGIN
           INSERT INTO INCORTA_INCREMENTAL.TableLastLoadTimestamp (TABLE_NAME,LOAD_TYPE,FILE_EPOCH) VALUES ('""" + synapse_schema_name + "." + synapse_table_name + """','""" + loadtype + """',""" + max_timestamp + """)
       END"""

    sql_statement.append(sql_create_schema)
    sql_statement.append(sql_create_table)
    sql_statement.append(sql_update_table)

    sql_string = ''.join([str(elem) for elem in sql_statement])
    #logging.info('SQL string for updating last incremental load timestamp is ' + sql_string)

    return sql_string


def incremental_merge_statement(sql_dict, tablename, schemaname, loadtype):
    sql_statement = []

    pkindex = sql_dict.query('is_key_column=="YES"')['column_name']
    columns = sql_dict['column_name']
    columns_to_update = \
    sql_dict.query('~column_name.str.contains("W_INSERT_DT") and ~column_name.str.contains("W_CREATED_BY")')[
        'column_name']
    sql_create_dedupe_tbl = create_table_statement(sql_dict, tablename, schemaname, loadtype, True)
    sql_statement.append(sql_create_dedupe_tbl)

    # Create SQL MERGE statement to hnadle insert and update
    logging.info("Merge Statement begins")
    #V1.3.8.2 - using the newly created incremental table for MERGE
    sql_merge = """MERGE {inc_tbl} t1 USING {inc_stg_tbl} t2 ON """.format(inc_tbl=schemaname + "." + tablename,
                                                                           inc_stg_tbl=schemaname + ".inc_" + tablename + "_new")
    sql_join_con = create_PK_index_statement(pkindex, tablename, schemaname, loadtype, True, False)
    sql_statement.append(sql_merge)
    sql_statement.append(sql_join_con)
    sql_not_match_insert = """ WHEN NOT MATCHED THEN INSERT ("""
    sql_statement.append(sql_not_match_insert)
    i = 1
    for index, item in columns.iteritems():
        sql_statement.append(item)
        if i < columns.size:
            sql_statement.append(',')
            i = i + 1
        else:
            sql_statement.append(')')

    sql_not_match_values = """VALUES ("""
    sql_statement.append(sql_not_match_values)
    i = 1
    for index, item in columns.iteritems():
        sql_statement.append('t2.' + item)
        if i < columns.size:
            sql_statement.append(',')
            i = i + 1
        else:
            sql_statement.append(')')
    sql_match_update = """ WHEN MATCHED THEN UPDATE """
    sql_statement.append(sql_match_update)
    sql_statement.append('SET ')
    i = 1
    for index, item in columns_to_update.iteritems():
        sql_statement.append(item + '=' + 't2.' + item)
        if i < columns_to_update.size:
            sql_statement.append(',')
            i = i + 1
        else:
            sql_statement.append(';')
    #sql_drop_table = "DROP TABLE " + schemaname + ".inc_" +  tablename + "_new; DROP TABLE " + schemaname + ".inc_" +  tablename + "_stg;"
    #sql_statement.append(sql_drop_table)
    sql_string = ''.join([str(elem) for elem in sql_statement])
    return sql_string


def get_latest_inc_file_location(service_client, fsystem, fpath, tablename, schemaname, synapse_table_name,
                                 synapse_schema_name, loadtype, forced_ts):
    logging.info("Retrieving latest file from ADLS for " + tablename)
    inc_timestamp_list = []
    new_inc_batch_id = []
    inc_load_set = True
    try:
        file_system_client = service_client.get_file_system_client(file_system=fsystem)

        paths = file_system_client.get_paths(path=fpath)
        logging.info("Searching in the directory -" + fpath + " for " + tablename)
        file_string = "/part-00000"
        #file_counter = 0

        for path in paths:
            #file_counter = file_counter + 1

            if path.name.find(tablename) != -1:
                try:
                    # if file_string in path.name:
                    if path.name.find(file_string) != -1:

                        split_string = path.name.split('.')

                        if split_string[1] is not None:
                            try:
                                inc_timestamp = split_string[1].split('/')[0]
                                # checking for temp files. Remove this line of code once engg team fixes the issue with temp folder
                                inc_timestamp = inc_timestamp.replace('temp_', '').replace('_' + tablename, '')
                                int(inc_timestamp)
                                inc_timestamp_list.append(inc_timestamp)
                            except:
                                pass
                except:
                    pass
    except Exception as error:
        logging.critical('Exception while fetching getting latest epoch time for incremental load - ' + str(error))

    listToStr = ' '.join([str(elem) for elem in inc_timestamp_list])
    # logging.info(str(file_counter) + ' sub directory and files present in the Incorta schema parquet root directory')
    # logging.info('Established connection with ADLS and retreived latest timestamp - ' + listToStr)

    if inc_timestamp_list and loadtype == 'Incremental':
        # sql_string = '''SELECT MAX(last_update_date) FROM ''' + schemaname + '.' + tablename
        
        sql_string = """SELECT MAX(FILE_EPOCH) FROM INCORTA_INCREMENTAL.TableLastLoadTimestamp WHERE TABLE_NAME='""" + synapse_schema_name + "." + synapse_table_name + """';"""  # V1.4
        # max_last_update_date = query_synapse(sql_string,True)

        # maxdte = dateutil.parser.parse(str(max_last_update_date))

        # maxts = time.mktime(maxdte.timetuple())*1000
        # maxts = 1602248668421
        
        if forced_ts:
            maxts = int(forced_ts)
            
        else:
            maxts = query_synapse(sql_string, True)

        # logging.info('Max date from synapse table = ' + str(maxdte) + 'Max timestamp = ' + str(maxts))
        logging.info('Max timestamp = ' + str(maxts))
        new_inc_batch_id = sorted(i for i in inc_timestamp_list if int(i) > maxts)
        return new_inc_batch_id, inc_load_set
    else:
        inc_load_set = False
        
        return inc_timestamp_list, inc_load_set


def create_adls_client():
    if adls_auth_method == 'serviceprincipal':
        credential = ClientSecretCredential(aad_tenant_id, aad_client_id, aad_client_secret)
    else:
        credential = "{}".format(adls_client_secret)
    try:
        adls_client = DataLakeServiceClient(
            account_url="{}://{}.dfs.core.windows.net".format("https", adls_account_name),
            credential=credential
        )
        return adls_client
    except Exception as error:
        logging.critical('Exception while establishing connection to ADLS - ' + str(error))


def connect_to_ADLS(tablename, schemaname, synapse_table_name, synapse_schema_name, loadtype, forced_ts):
    try:
        global service_client
        fname = ''
        latest_timestamp = []
        service_client = create_adls_client()
        # inc_load_set = is_incremental_load_set(service_client,tablename, schemaname)
        latest_timestamp, inc_load_set = get_latest_inc_file_location(service_client, adls_file_system,
                                                                      adls_root_dir + "/" + schemaname, tablename,
                                                                      schemaname, synapse_table_name,
                                                                      synapse_schema_name,
                                                                      loadtype, forced_ts)  # V1.4 - adding 2 additional parameters for custom target schema

        # logging.info("Timestamp retrieved with max value " + str(max(latest_timestamp)) + ",min values " + str(min(latest_timestamp)))
    except Exception as error:
        logging.critical('Exception while establishing connection to ADLS - ' + str(error))

    return latest_timestamp, inc_load_set


def fetch_parquet_metadata(handler, path):
    """
    Used to fetch the metadata from the specified parquet path.
    Example: fetch_parquet_metadata(handler, "Tenants/ebs_cloud/parquet/EBS_GL/GL_BALANCES/part-000000")
    :param handler: adls filesystem handler to read from the adls bucket
    :param path: full path to the paruqet file
    :return: list of list where each list is of the form [column_name, column_type]
    """
    logging.info("Fetching <{}> meta".format(path))
    try:
        ret = []
        with handler.open_input_stream(path) as inp:
            pq_meta = pq.read_metadata(inp)
            columns = pq_meta.num_columns
            for i in range(columns):
                # read column informtaion from column information rather than row_group
                #column_meta = pq_meta.row_group(0).column(i)
                #column_name = column_meta.path_in_schema
                column_meta = pq_meta.schema.column(i)
                column_name = column_meta.name
                column_type = column_meta.physical_type
                #if column_meta.statistics is not None:
                #    if column_meta.statistics.logical_type.type != "NONE":
                #        column_type = column_meta.statistics.logical_type.type
                if column_meta.logical_type.type != "NONE":
                    column_type = column_meta.logical_type.type
                ret.append([column_name, column_type])
        logging.info("Fetched <{}> meta and retrieved {} columns: {}".format(path, len(ret), ' '.join([str(x) for x in ret])))
        return ret
    except Exception as e:
        logging.error("Failed to get <{}> meta due to: {}".format(path, e))
        return []


def fetch_parquet_paths(handler, path):
    return [x.name for x in handler.get_paths(path) if str.startswith(x.name.split('/')[-1], "part-")]


def fetch_parquet_metadata_from_paths(handler, paths):
    for path in paths:
        meta = fetch_parquet_metadata(handler, path)
        if meta:
            return meta
    return []


def fetch_tables(handler, schema, tables_to_fetch=None):
    """
    Discovers the table names and the paths to their latest increments in a schema directory unless tables_to_fetch is
    defined where it will only fetch the latest increment for the specified tables
    Example: fetch_parquet_metadata(handler, "EBS_GL")
    Example: fetch_parquet_metadata(handler, "EBS_GL", tables_to_fetch=["GL_BALANCES"])
    :param handler: adls filesystem handler to read from the adls bucket
    :param schema: schema name
    :param tables_to_fetch: list of specific tables to fetch
    :return: dictionary where every key is the table name and the value is the path to the latest increment folder
    """
    ret = {}
    tables = [x.name.split('/')[-1].split('.') for x in handler.get_paths(path="{}/{}".format(adls_root_dir, schema)) if "Load_Synapse" not in x.name]
    if tables_to_fetch is not None:
        tables = [x for x in tables if x[0] in tables_to_fetch]
    for table in tables:
        qualified_name = schema + "." + table[0]
        if len(table) == 1:
            table.append("-1")

        if qualified_name not in ret or int(table[1]) > int(ret[qualified_name][1]):
            path = "{}/{}/{}".format(adls_root_dir, schema, table[0])
            if table[1] != "-1":
                path += ".{}".format(table[1])
            ret[qualified_name] = [path, table[1]]
    return ret


def fetch_table_paths(handler, schema, table_to_fetch):
    ret = []
    tables = [x.name.split('/')[-1].split('.') for x in handler.get_paths(path="{}/{}".format(adls_root_dir, schema)) if "Load_Synapse" not in x.name]
    tables = [x for x in tables if x[0] == table_to_fetch]
    for table in tables:
        ret.append("{}/{}/{}".format(adls_root_dir, schema, table[0]))
    return ret


def merge_sqli_pq_meta(sqli_meta, pq_meta):
    """
    merge the 2 objects generated from fetch_parquet_metadata and connect_to_incorta to replace the type data with the
    one fetched from the parquet

    :param sqli_meta: metadata object from connect_to_incorta
    :param pq_meta: metadata object from fetch_parquet_metadata
    :return:
    """
    ret = {}
    for table in sqli_meta:
        try:
            meta = []
            for sqli_column in sqli_meta[table]:
                pq_column = [x for x in pq_meta[table] if x[0] == sqli_column[0]]
                if pq_column:
                   meta.append((sqli_column[0], pq_column[0][1], sqli_column[2]))
                else:
                    logging.info("Skipped Meta Merge of column <{}.{}>".format(table, sqli_column[0]))
            ret[table] = meta
            logging.info("Merged meta for table <{}>".format(table))
        except Exception as e:
            logging.error("Skipped Meta Merge for table <{}> due to: {}".format(table, e))

    return ret


def is_sampling_enabled():
    try:
        sampling_enabled = spark.conf.get("spark.dataframe.sampling.enabled")
        return sampling_enabled.lower() != "false"
    except:
        return True


def mask_query(to_mask):
    masked = re.sub(r"CREDENTIAL=\((.+?)\)", "CREDENTIAL=*", to_mask)
    return masked


def _load_to_synapse(schema_or_tables, loadtype, forced_ts):
    '''
     Loads data from incorta to Synapse.

     Parameters
     ----------
     schema_or_tables : list or str
         Either the name of a schema, or a list of fully qualified table names (schemaName.tableName) to be loded.
     loadtype : str
         Determines the load type, either 'Full' load or 'Incremental' load.

     Returns
     -------
     df : Spark dataframe
         A Spark dataframe containing the status of the loaded tables.
    '''
    if loadtype not in ['Full', 'Incremental']:
        raise Exception("Unknown specified load type {}. Please use either `Full` or `Incremental`.".format(loadtype))

    status = []
    #V1.3.8.2 - separate stg and main tbl sql
    #status_schema = ['last_run', 'load_type', 'table', 'rows_status', 'create_stg_tbl_sql', 'copy_tbl_sql', 'create_main_tbl_sql', 'merge_tbl_sql', 'updt_ts_sql']
    
    #V1.3.8.3 - update MV columns with better logging
    #status_schema = ['start_ts', 'end_ts', 'load_type', 'schema_name','tbl_name', 'extracted_rows' ,'loaded_rows' , 'run_status' , 'create_stg_tbl_sql', 'copy_tbl_sql', 'create_main_tbl_sql', 'merge_tbl_sql', 'updt_ts_sql']
    status_schema = StructType([StructField('start_ts',TimestampType()),
                    StructField('end_ts',TimestampType()),
                    StructField('load_type',StringType()),
                    StructField('schema_name',StringType()),
                    StructField('tbl_name',StringType()),
                    StructField('extracted_rows',IntegerType()),
                    StructField('loaded_rows',IntegerType()),
                    StructField('run_status',StringType()),
                    StructField('create_stg_tbl_sql',StringType()),
                    StructField('copy_tbl_sql',StringType()),
                    StructField('create_main_tbl_sql',StringType()),
                    StructField('merge_tbl_sql',StringType()),
                    StructField('updt_ts_sql',StringType()),
                    StructField('last_inc_file_ts',StringType()),
                    StructField('last_inc_folder_path',StringType())])
    msg = ''
    msg2 = ''
    msg3 = ''
    msg4 = ''
    #V1.3.8.2 - additional msg
    msg5 = ''
    rows_extracted = rows_loaded = 0
    sql_error_msgs = ["Exception", "error", "Error","[Microsoft][ODBC Driver 17 for SQL Server]"]
    copy_tbl_sql = ''
    #V1.3.8.2 - separate stg and main tbl sql
    create_stg_tbl_sql = ''
    create_main_tbl_sql = ''
    merge_tbl_sql = ''
    updt_ts_sql = ''
    uts_sql = ''
    job_id = uuid.uuid4()
    load_start_ts = datetime.now()
    adls_file_path = ''
    last_inc_file_ts = ''
    last_inc_folder_loc = ''
    #V1.3.8.2 - separate stg and main tbl created ts
    load_end_ts = stg_tbl_created_ts = main_tbl_created_ts = tbl_copied_ts = tbl_merged_ts = load_start_ts
    adls_client = create_adls_client().get_file_system_client(adls_file_system)
    adls_fs = pyfs_adls.FilesystemHandler.from_filesystem_client(
        adls_client)  # create pyarrow_fs handler from datalake client

    tables_dicts = {}
    if type(schema_or_tables) == list:
        for elem in schema_or_tables:
            try:
                schemaname, tablename = elem.split(".")
            except:
                err_msg = "Invalid table name [{}]. Table name should be fully qualified (schemaName.tableName)."
                logging.error(err_msg)  # V1.4
                raise Exception(err_msg)

            qualified_name = "{}.{}".format(schemaname, tablename)
            try:
                pq_paths = fetch_tables(adls_fs, schemaname, [tablename])[qualified_name] # fetch table's latest increment path
                pq_meta = {qualified_name: fetch_parquet_metadata_from_paths(adls_fs, fetch_parquet_paths(adls_fs, pq_paths[0]))}  # parquet's metadata
                sqli_meta = connect_to_incorta(schemaname, tablename)
                tables_dicts.update(merge_sqli_pq_meta(sqli_meta, pq_meta))  # merge parquet metadata with sqli metadata
            except:
                #V1.3.8.2 - additional param
                status.append(load_start_ts, load_end_ts , loadtype, schemaname, tablename, 0, 0, "Data Discovery Skipped", "SKIPPED", "SKIPPED", "SKIPPED", "SKIPPED", "SKIPPED",0,"SKIPPED")
                logging.error("Skipped discovery of table <{}>".format(qualified_name))
                #V1.3.8.2 - additional param
                csvlogger.info([incorta_db_name,job_id,schemaname,tablename,synapse_database,'NA','NA',loadtype,"0",load_start_ts,stg_tbl_created_ts,tbl_copied_ts,main_tbl_created_ts,tbl_merged_ts,load_end_ts,"0",incorta_user,loaderuser])

    elif type(schema_or_tables) == str:
        if '.' in schema_or_tables:
            raise Exception(
                "Invalid schema name [{}]. Input should either be a schema name or a list of fully qualified table names.".format(schema_or_tables))

        pq_meta = {}
        try:
            pq_paths = fetch_tables(adls_fs, schema_or_tables)
        except:
            logging.error("Skipped discovery of schema <{}>".format(schema_or_tables))
            pq_paths = []

        for qualified_name in pq_paths:
            try:
                pq_meta[qualified_name] = fetch_parquet_metadata_from_paths(adls_fs, fetch_parquet_paths(adls_fs, pq_paths[qualified_name][0]))
            except:
                #V1.3.8.2 - additional param
                status.append((load_start_ts, load_end_ts, loadtype,qualified_name.split(".")[0] , qualified_name.split(".")[-1],0,0, "Data Discovery Skipped", "SKIPPED","SKIPPED", "SKIPPED", "SKIPPED", "SKIPPED",0,"SKIPPED"))
                logging.error("Skipped discovery of table <{}>".format(qualified_name))
                #V1.3.8.2 - additional param
                csvlogger.info([incorta_db_name,job_id,'NA',qualified_name.split(".")[-1],synapse_database,'NA','NA',loadtype,"0",load_start_ts,stg_tbl_created_ts,tbl_copied_ts,main_tbl_created_ts,tbl_merged_ts,load_end_ts,"0",incorta_user,loaderuser])

        sqli_meta = connect_to_incorta(schema_or_tables)

        tables_dicts = merge_sqli_pq_meta(sqli_meta, pq_meta)
    else:
        raise Exception("Input should either be a schema name or a list of fully qualified type name.")

    if is_sampling_enabled():
        #V1.3.8.2 - additional param
        return spark.createDataFrame(
            [[load_start_ts, load_end_ts, "D", "D", "D", 0, 0, "D", "D", "D", "D", "D", "D", "D", "D"]],
            status_schema
        )

    logging.info("Tables to be loaded (excludes SKIPPED tables) : " + str(tables_dicts))
    # repeat load for each table in the table list
    
    for item, get_dictionary in tables_dicts.items():
        load_start_ts = datetime.now()
        tablename = item.split(".")[1]
        schemaname = item.split(".")[0]
        adls_file_path = "{}://{}.dfs.core.windows.net/{}/{}/{}/".format(
        "https", adls_account_name, adls_file_system, adls_root_dir, schemaname)
        last_inc_folder_loc = ''
        last_inc_file_ts = ''
        # V1.4 to set a customer schema name for Synapse
        synapse_schema_name = ""
        synapse_table_name = ""
        if synapse_target_schema_name != "<I_SCHEMA>" and synapse_target_table_name == "<I_TABLE>":
            synapse_schema_name = synapse_target_schema_name
            synapse_table_name = tablename
        elif synapse_target_schema_name != "<I_SCHEMA>" and synapse_target_table_name != "<I_TABLE>":
            start = '<I_SCHEMA>'
            end = '<I_TABLE>'
            s = synapse_target_table_name
            delimiter = s[s.find(start) + len(start):s.rfind(end)]
            synapse_schema_name = synapse_target_schema_name
            synapse_table_name = schemaname + delimiter + tablename
        else:
            synapse_schema_name = schemaname
            synapse_table_name = tablename
        
        table_hash_col = get_hash_column(schemaname, tablename)
        if table_hash_col is not None:
            set_hash_column(synapse_schema_name, synapse_table_name, table_hash_col)

        if not get_dictionary:
            logging.error('No column information could be retrieved from Incorta for ' + schemaname + '.' + tablename)
            stg_tbl_created_ts=tbl_copied_ts=main_tbl_created_ts=tbl_merged_ts=load_end_ts = datetime.now()
            #V1.3.8.2 - additional param
            status.append((load_start_ts, load_end_ts, loadtype, schemaname, tablename,0, 0,
                        "Data Discovery Failed",
                        'SKIPPED',"SKIPPED", 'SKIPPED', 'SKIPPED', 'SKIPPED',0,'SKIPPED'))
            
            rows_extracted = rows_loaded = 0
            #V1.3.8.2 - additional param
            csvlogger.info([incorta_db_name,job_id,schemaname,tablename,synapse_database,synapse_schema_name,synapse_table_name,loadtype,rows_extracted,load_start_ts,stg_tbl_created_ts,tbl_copied_ts, main_tbl_created_ts, tbl_merged_ts,load_end_ts,rows_loaded,incorta_user,loaderuser])
        else:
            sql_dict = pd.DataFrame(get_dictionary, columns=['column_name', 'incorta_dtype', 'is_nullable'])
            null_count = sql_dict['is_nullable'].str.contains('NO').sum()
            logging.info("PK count " + str(null_count) + " for " + tablename)
            # map incorta data types to ms sql data types
            dtype = {
                'incorta_dtype': ['STRING', 'BYTE_ARRAY', 'TIMESTAMP', 'INT32', 'INT64', 'DOUBLE', 'DATE'],
                'mssql_dtype': ['[nvarchar](4000)', '[nvarchar](4000)', '[datetime]', '[int]', '[bigint]', '[float]', '[date]'],
                'default': ['not_set', 'not_set', '1971-01-01 00:00:00', '0', '0', '0.0', '1971-01-01']
            }
            sql_dtype_ref = pd.DataFrame(data=dtype)
            sql_dict = sql_dict.merge(sql_dtype_ref, how='left', on='incorta_dtype', sort=False)
            # add cdc related columns
            sql_dict.loc[sql_dict['is_nullable'] == 'NO', 'is_key_column'] = 'YES'
            sql_dict.loc[sql_dict['is_nullable'] == 'YES', 'is_key_column'] = 'NO'
            cdc_columns = {'column_name': ['W_INSERT_DT', 'W_CREATED_BY', 'W_UPDATE_DT', 'W_UPDATED_BY'],
                            'incorta_dtype': ['', '', '', ''], 'is_nullable': ['NO', 'NO', 'NO', 'NO'],
                            'mssql_dtype': ['[datetime]', '[nvarchar](4000)', '[datetime]', '[nvarchar](4000)'],
                            'default': [datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3], loaderuser,
                                        datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3], loaderuser],
                            'is_key_column': ['NO', 'NO', 'NO', 'NO']}
            sql_cdc_ref = pd.DataFrame(data=cdc_columns)
            sql_dict = pd.concat([sql_dict, sql_cdc_ref], ignore_index=True)
            sql_dict['column_name'] = '[' + sql_dict['column_name'] + ']' #To allow use of SQL reserved keywords as column names
            if loadtype == 'Full':
                logging.info('Full Load Triggered for : ' + tablename)

                #ct_sql = create_table_statement(sql_dict, synapse_table_name, synapse_schema_name, loadtype, upsert=False)  # V1.4 - definition updated to set a custom target schema name
                #V1.3.8.2 - new function for creating table in round-robin distribution
                ct_sql = create_stg_load_table_statement(sql_dict, synapse_table_name, synapse_schema_name, loadtype)
                msg = query_synapse(ct_sql, False)
                stg_tbl_created_ts = datetime.now()
                cc_sql = create_copy_statement(sql_dict, tablename, schemaname, synapse_table_name, synapse_schema_name, loadtype, 0)  # V1.4 - added additional 2 parameters to the definition for custom target schem name
                
                msg2 = query_synapse(cc_sql, False)
                if any(error in str(msg2) for error in sql_error_msgs):
                    rows_extracted = 0
                else:
                    rows_extracted = int(str(msg2).replace(',',' ').split('.')[0].strip())
                msg3 = msg2
                rows_loaded = rows_extracted
                tbl_copied_ts =  datetime.now()
                cth_sql = create_table_statement(sql_dict, synapse_table_name, synapse_schema_name, loadtype, upsert=False)
                msg4 = query_synapse(cth_sql,False)
                if any(error in str(msg4) for error in sql_error_msgs):
                    rows_loaded = 0
                main_tbl_created_ts = tbl_merged_ts = datetime.now()
                if rows_loaded !=0:
                    rename_sql = rename_tbl_statement(synapse_schema_name,synapse_table_name)
                    msg6 = query_synapse(rename_sql, False)
                    logging.info("Tble Rename Completed: " + str(msg6))
                    drop_sql = delete_tbl_statement(synapse_schema_name,synapse_table_name)
                    msg7 = query_synapse(drop_sql, False)
                    logging.info("Tble Deletion Completed: " + str(msg7))
                    uts_sql = update_timestamp_statement("0", synapse_table_name, synapse_schema_name, loadtype)  # V1.4
                    msg5 = query_synapse(uts_sql, False)
                    last_inc_file_ts = "0"
                    last_inc_folder_loc = adls_file_path + tablename
                else:
                    msg5 = "Timestamp table not updated"
                
                copy_tbl_sql = mask_query(cc_sql)
                create_stg_tbl_sql = ct_sql
                create_main_tbl_sql = cth_sql
                updt_ts_sql = uts_sql
            elif loadtype == 'Incremental':
                logging.info('Incremental Load Triggered')

                inc_timestamp, inc_load_set = connect_to_ADLS(tablename, schemaname, synapse_table_name, synapse_schema_name, loadtype, forced_ts)  # V1.4

                if inc_timestamp and inc_load_set:
                    
                    ct_sql = create_stg_load_table_statement(sql_dict, synapse_table_name, synapse_schema_name, loadtype)  # V1.4
                    msg = query_synapse(ct_sql, False)
                    logging.info(
                        "If the file for Incremental Load exist in ADLS, timestamp information is shown here with max value " + str(
                            max(inc_timestamp)) + ",min values " + str(min(inc_timestamp)))
                    stg_tbl_created_ts = datetime.now()
                    cc_sql = create_copy_statement(sql_dict, tablename, schemaname, synapse_table_name,
                                                    synapse_schema_name, loadtype, inc_timestamp)  # V1.4
                    msg2 = query_synapse(cc_sql, False) 
                    tbl_copied_ts = datetime.now()
                    if any(error in str(msg2) for error in sql_error_msgs):
                        rows_extracted = 0
                    else:
                        rows_extracted = int(str(msg2).replace(',',' ').split('.')[0].strip())
                    iu_sql = incremental_merge_statement(sql_dict, synapse_table_name, synapse_schema_name,
                                                            loadtype)  # V1.4
                    msg3 = query_synapse(iu_sql, False)
                    if any(error in str(msg3) for error in sql_error_msgs):
                        rows_loaded = 0
                    else:
                        rows_loaded= int(str(msg3).replace(',',' ').split('.')[0].strip())
                    msg4 = msg3
                    if rows_loaded !=0:
                        drop_sql = rename_tbl_statement(synapse_schema_name,"inc_" + synapse_table_name)
                        msg6 = query_synapse(drop_sql, False)
                        logging.info("Tble Drop Completed for inc_<tbl_name>_new: " + str(msg6))
                        drop_sql = delete_tbl_statement(synapse_schema_name,"inc_" + synapse_table_name)
                        msg7 = query_synapse(drop_sql, False)
                        logging.info("Tble Drop Completed for inc_<tbl_name>_stg: " + str(msg7))
                        uts_sql = update_timestamp_statement(str(max(inc_timestamp)), synapse_table_name,
                                                                synapse_schema_name, loadtype)  # V1.4
                        msg5 = query_synapse(uts_sql, False)
                        last_inc_file_ts = str(max(inc_timestamp))
                        last_inc_folder_loc = adls_file_path + tablename + "." + last_inc_file_ts
                    else:
                        msg5 = "Timestamp table not updated"
                    tbl_merged_ts = datetime.now()
                    copy_tbl_sql = mask_query(cc_sql)
                    create_stg_tbl_sql = ct_sql
                    create_main_tbl_sql = ""
                    merge_tbl_sql = iu_sql
                    updt_ts_sql = uts_sql

                elif not (inc_load_set) and null_count != 0:
                    
                    msg2 = "0.There are no new incremental files available for updation (Or) Synapse is paused"
                    msg3 = "0"
                    rows_extracted = rows_loaded = 0
                    logging.info("Load Aborted as there is no incremental batch file")
                    stg_tbl_created_ts = main_tbl_created_ts= tbl_copied_ts = tbl_merged_ts = str(
                    datetime.now())

                elif not (inc_load_set) or null_count==0: #V1.4 - Full load when no incremental set or no keys identified in Incorta
                    
                    ct_sql = create_stg_load_table_statement(sql_dict, synapse_table_name, synapse_schema_name, "Full")  # V1.4 - definition updated to set a custom target schema name

                    msg = query_synapse(ct_sql, False)
                    stg_tbl_created_ts = datetime.now()
                    cc_sql = create_copy_statement(sql_dict, tablename, schemaname, synapse_table_name, synapse_schema_name, "Full", 0)  # V1.4 - added additional 2 parameters to the definition for custom target schem name

                    msg2 = query_synapse(cc_sql, False)
                    if any(error in str(msg2) for error in sql_error_msgs):
                        rows_extracted = 0
                    else:
                        rows_extracted = int(str(msg2).replace(',',' ').split('.')[0].strip())
                    msg3 = str(msg2) + ".This table will always load in Full only - No Incremental extraction query available (or) No keys defined"
                    rows_loaded = rows_extracted
                    curr_timestamp_list, inc_load_set = connect_to_ADLS(tablename, schemaname, synapse_table_name, synapse_schema_name, "Full", forced_ts)  # V1.4
                    curr_timestamp = 0
                    if not curr_timestamp_list:
                        logging.info("timestamp is empty")
                        curr_timestamp = int(time.time() * 1000)
                    else:
                        logging.info("timestamp is not empty")
                        curr_timestamp = max(curr_timestamp_list)

                    cth_sql = create_table_statement(sql_dict, synapse_table_name, synapse_schema_name, loadtype, upsert=False)
                    msg4 = query_synapse(cth_sql,False)
                    main_tbl_created_ts = datetime.now()
                    if any(error in str(msg4) for error in sql_error_msgs):
                        rows_loaded = 0
                    if rows_loaded !=0:
                        rename_sql = rename_tbl_statement(synapse_schema_name,synapse_table_name)
                        msg6 = query_synapse(rename_sql, False)
                        logging.info("Tble Rename Completed: " + str(msg6))
                        drop_sql = delete_tbl_statement(synapse_schema_name,synapse_table_name)
                        msg7 = query_synapse(drop_sql, False)
                        logging.info("Tble Deletion Completed: " + str(msg7))
                        uts_sql = update_timestamp_statement(str(curr_timestamp), synapse_table_name,
                                                                synapse_schema_name, "Full")  # V1.4
                        msg5 = query_synapse(uts_sql, False)
                        last_inc_file_ts = "0"
                        last_inc_folder_loc = adls_file_path + tablename 
                    else:
                        msg5 = "Timestamp table not updated"
                    tbl_copied_ts = tbl_merged_ts = datetime.now()
                    copy_tbl_sql = mask_query(cc_sql)
                    updt_ts_sql = uts_sql
                    create_stg_tbl_sql = ct_sql
                    create_main_tbl_sql = cth_sql

                else:
                    
                    msg2 = "0.There are no new incremental files available for updation (Or) Synapse is paused"
                    msg3 = "0"
                    rows_extracted = rows_loaded = 0
                    logging.info("Load Aborted as there is no incremental batch file")
                    stg_tbl_created_ts = main_tbl_created_ts =  tbl_copied_ts = tbl_merged_ts = datetime.now()

            else:
                msg = "No rows were extracted from source system. Aborting load to Synapse"
                stg_tbl_created_ts = main_tbl_created_ts = tbl_copied_ts = tbl_merged_ts = datetime.now()
            load_end_ts = datetime.now()
            status.append((load_start_ts ,load_end_ts, loadtype, schemaname, tablename, rows_extracted , rows_loaded,
                        "Extracted(COPY DATA) : " + loadtype + "=" + str(msg2) + ", " + "Loaded(MERGED) : " + str(msg3) + " Other Query Exec. Log : " + "Hash table created - " + str(msg4) + ".Timestamp table updated - " + str(msg5),
                        create_stg_tbl_sql, copy_tbl_sql, create_main_tbl_sql , merge_tbl_sql, updt_ts_sql,last_inc_file_ts,last_inc_folder_loc))
            
            csvlogger.info([incorta_db_name,job_id,schemaname,tablename,synapse_database,synapse_schema_name,synapse_table_name,loadtype,rows_extracted,load_start_ts,stg_tbl_created_ts,tbl_copied_ts,main_tbl_created_ts,tbl_merged_ts,load_end_ts,rows_loaded,incorta_user,loaderuser])
    if not status:
        stg_tbl_created_ts = main_tbl_created_ts = tbl_copied_ts = tbl_merged_ts = load_end_ts = load_start_ts
        status.append((load_start_ts, load_end_ts, loadtype, str(schema_or_tables), str(schema_or_tables), 0,0, "No Table to Load","SKIPPED", "SKIPPED", "SKIPPED", "SKIPPED", "SKIPPED",0,"SKIPPED"))
        
        csvlogger.info([incorta_db_name,job_id,schema_or_tables,schema_or_tables,synapse_database,"SKIPPED","SKIPPED",loadtype,rows_extracted,load_start_ts,stg_tbl_created_ts,tbl_copied_ts,main_tbl_created_ts,tbl_merged_ts,load_end_ts,rows_loaded,incorta_user,loaderuser])
    return spark.createDataFrame(status, status_schema)


def load_to_synapse(schema_or_tables, loadtype):
    build_synapse_mappings()
    if loadtype == 'Full':
        df1 = _load_to_synapse(schema_or_tables, 'Full', None)
        df2 = _load_to_synapse(schema_or_tables, 'Incremental', None)
        return df1.union(df2)
        #return _load_to_synapse(schema_or_tables, 'Full', None)
    elif loadtype == 'Incremental':
        return _load_to_synapse(schema_or_tables, 'Incremental', None)