import time
import logging
from typing import List, Optional, Tuple
import pandas as pd

import click
from common.db.utils import create_postgresql_engine
import psycopg2
from common.commands import options as commonoptions
from common.apis.cpq import CpqApi, CpqApiError
from os import  path
import io

from .cpqfeed import cli
from . import options

OFFSET_NOT_SET = -1

log = logging.getLogger(__name__)

@cli.command()
@options.p360_db_host
@options.p360_db_port
@options.p360_db_name
@options.p360_db_user
@options.p360_db_password
@options.download_directory
@commonoptions.limit

@click.option('--offset', type=int, default=OFFSET_NOT_SET,
              help="Data item number to start fetching from.")
@click.pass_context
def emea_data_extract(ctx, p360_db_host, p360_db_port, p360_db_name,
                      p360_db_user, p360_db_password, download_directory, limit, offset):
    """
    Pull data from CPQ API into P360 DB.

    To manually set/reset the offset for the CPQ Api, add
    "--offset <int_value_of_offset>" to the command.

    >>> USAGE: cpqfeed emea_data_extract
    """

    p360_db_user = "".join([p360_db_user, "1"])

    cpq_api = ctx.obj['api']
    p360_engine = create_postgresql_engine(
        host=p360_db_host,
        port=p360_db_port,
        name=p360_db_name,
        user=p360_db_user,
        password=p360_db_password,
    )

    # get start time
    start_time = time.time()
    log.info(f"Job Started at  {start_time} \n exporting data from CPQ ---")
    
    emea_data_extract_impl(cpq_api)
    cpy_csv_to_tble(cpq_api, p360_engine, download_directory)

    # get the end time
    end_time = time.time()

    # get the processed time
    time_interval =  end_time - start_time

    log.info(f"Loading data process has been Completed at {end_time}")
    log.info(f"Time required to load the data is => {time_interval} seconds.")


def emea_data_extract_impl(cpq_api):
    """
    Data export and Data loding Process.
    :return:
    """
    log.info("Step 1. Get the list of attrToPart table names - CPQ")
    tables_names = list_of_tables(cpq_api)
    if not tables_names:
        log.error(f"No tables names from CPQ. Quitting...")
        return

    # get the taskid and status of response
    task_id, status = get_task_id(cpq_api, tables_names)

    if not status:
        log.error('No status from CPQ')
        return

    response = cpq_api.list_of_file_links(task_id)
    items = response.json()['items'][0]['links']
    datatable_link = items[0]['href']
    log.info(f"link of data table {datatable_link}")
    cpq_api.download_zip_file(datatable_link, task_id)

def list_of_tables(cpq_api: CpqApi) -> List[Optional[str]]:
    """
    Get the list of tables from CPQ mapping and returns the list of their names
    """
    list_of_tables = []
    try:
        response = cpq_api.fetch_cpq_tables_mapping()
    except CpqApiError:
        log.exception(f"Failed to retrieve the data from CPQ tables mapping")
        return []
    else:
        if response.status_code < 200 or response.status_code > 399:
            log.error(f"Bad status code for the CPQ response: {response.content}")
            return []
        res_items = response.json()['items']
        _ = [
            list_of_tables.append(
                i['table_name']) for i in res_items if i['table_name'] not in list_of_tables
        ]
        log.info(f"Got list of tables names {list_of_tables} for CPQ")
    return list_of_tables

def get_task_id(cpq_api, list_of_tables):
    """
    Submit the Data export task.
    Get the taskid of completed task.
    :return: taskid
    """
    try:
        response = cpq_api.export_data(list_of_tables)
        response_json = response.json()
        log.debug(f'Task id is {response_json}')

        if 'Error' in response_json:
            err_msg = f"CPQ API call failed with error: {response_json.get('Error')}"
            log.error(err_msg)
            raise CpqApiError(err_msg)
        else:
            taskid = response_json['taskId']

            # get status of task id
            status = cpq_api.get_task_status(taskid)
            log.info(status)
    except CpqApiError as err:
        log.exception(f'CPQ API execution failed with the error {err}')

    log.info(f"Status for Task Id {taskid} generated for the list attR tables is {status}.")
    return taskid, status

def csv_cols_mapping(cpq_api, download_directory):

    # get list of files
    f_list = cpq_api.get_list_of_files(download_directory)

    f_dict = {}
    for f in f_list:
        f = path.join(download_directory, f)

        # get the column names, column types and total columns size
        cols, types, cols_num  = get_csv_cols(cpq_api, f)

        f_dict[f] = [cols, types, cols_num]
        # create a list by sorting the dict on column size
        sort_f_list = sorted(f_dict.items(), key=lambda x: x[1])

        # get the csv having maximum columns
        get_maxcol_csv = sort_f_list[-1]
        # list of files
        file_list = f_dict.keys()

    return file_list, get_maxcol_csv

def get_csv_cols(cpq_api, csvfile):
    """
    read file and return
    cols, type, total column size
    """
    log.info(f"Reading csv in data frame Object.")
    csv_file = pd.read_csv(csvfile)

    # fetch the first header
    table_cols = csv_file.iloc[[0]]
    table_col_types = csv_file.iloc[[1]]

    # collect the tables column names in the list data structure
    table_cols = table_cols.to_string().split()
    table_col_types = table_col_types.to_string().split()

    col_size = len(table_cols[3:])
    return table_cols[3:], table_col_types[3:], col_size

# function to create  table
def create_attr_table(cpq_api, p360_engine, csv_header_data ):
    """
    Creating table by reading csv header as column names.
    """
    # table name to load the data
    table_name = "p360.temp_attrTable"

    log.info(f"table column {csv_header_data}")
    col_size = csv_header_data[1][2]

    # creating dict{} with cols, data type, null
    data = { 'table_cols' : csv_header_data[1][0],
         'cols_data_type' : csv_header_data[1][1],
         'null' : 'NULL'
         }

    # creating dataframe of cols, datatype, null
    df = pd.DataFrame(data)

    # list of values
    df_list = df.values.tolist()
    log.info(df_list)

    # creating list of tuples from list of list
    list_of_tuples = [ tuple(i) for i in df_list ]
    log.info(list_of_tuples)

    # creating list of tuples[ (col dataype null), (col, datatype null) ...]
    sql_string = list(map(cpq_api.join_elements, list_of_tuples))
    string = " ".join(sql_string).replace('String', 'VARCHAR')[:-1]

    # creating table with indices
    try:
        with p360_engine.begin() as transact:

            log.info(f'Dropping and Creating table {table_name}')
            transact.execute(f"DROP TABLE if exists {table_name}")
            transact.execute(f"CREATE TABLE IF NOT EXISTS  {table_name} ({string})")
    
            log.info(f'Creating indices on the table {table_name}')
            transact.execute(f"CREATE INDEX IF NOT EXISTS temp_attrtable_model_idx ON {table_name} USING btree (model)")
            transact.execute(f"CREATE INDEX IF NOT EXISTS temp_attrtable_attrgroup_idx ON {table_name} USING btree (attrgroup);")
            transact.execute(f"CREATE INDEX IF NOT EXISTS temp_attrtable_partnumber_idx ON {table_name} USING btree (partnumber);")

    except Exception:
        log.exception(f'Failed to drop and create the table {table_name}')
    else:
        log.info(f"Dropped and created the table {table_name}")
    return table_name, col_size

def cpy_csv_to_tble(cpq_api, p360_engine, download_directory):
    """
    load data from csv to created table.
    """
    list_of_files, csv_header_data = csv_cols_mapping(cpq_api, download_directory)

    # creating table
    attr_table, column_size = create_attr_table(cpq_api, p360_engine, csv_header_data)

    for csv_file in list_of_files:

        log.info(f"Processing data loading process for {csv_file}")
        tbl_col, col_type, col_size = get_csv_cols(cpq_api, csv_file)
        log.info(f"file {csv_file} is having columns {col_size}")

        s = open(csv_file, 'r').read()
        # to escape characters , chars = ('$','%','^','*','\\') # etc
        chars = ('\\')  # etc
        for c in chars:
            s = ''.join(s.split(c))

        # logic to make csv (having less columns), compatible with other csv's
        if column_size != col_size:
            log.info("difference found in columns size.")
            get_col_diff = column_size - col_size

            delimiter_str = "," * get_col_diff + "\n"
            log.info(f"adding {get_col_diff} columns in the file {csv_file}")
            s = s.replace('\n', delimiter_str)
            s = s.rstrip("\n")

        out_file = io.StringIO()
        out_file.write(s)
        out_file.seek(0)

        for _ in range(5):
            next(out_file)
        connection = p360_engine.raw_connection()
        with connection.cursor() as cursor:
            cursor.copy_from(out_file, attr_table, sep=',', null='None')
        connection.commit()


