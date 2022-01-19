from csv_logger import CsvLogger
import logging
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

filename =  spark.conf.get("spark.Logs.csvfilepath")
#filename = '/home/incorta/IncortaAnalytics/IncortaNode/syn/logs/log.csv'
level = logging.INFO
fmt = '%(asctime)s,%(message)s'
datefmt = '%Y/%m/%d %H:%M:%S'
max_size = 1048576 # 1 MB files
max_files = 4 # 4 rotating files
header = ['log_ts'
,'incorta_tenant_name'
,'jobid'
,'source_schema_name'
,'source_table_name'
,'synapse_db_name'
,'target_schema_name'
,'target_table_name'
,'loadtype'
,'rowcount_extracted'
,'load_start_ts'
,'stg_tbl_created_ts'
,'tbl_copied_ts'
,'main_tbl_created_ts'
,'tbl_merged_ts'
,'load_end_ts'
,'rowcount_loaded'
,'incorta_user_id'
,'synapse_loader_user_id']

# Creat logger with csv rotating handler
csvlogger = CsvLogger(
    filename=filename,
    level=level,
    fmt=fmt,
    datefmt=datefmt,
    max_size=max_size,
    max_files=max_files,
    header=header
    )

