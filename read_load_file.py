import time
import logging
import pandas as pd
from typing import List, Optional, Tuple
from os import path
import click
from common.db.utils import create_postgresql_engine
from common.commands import options as commonoptions
from common.db.utils import create_postgresql_engine

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
@click.pass_context

def read_load_file(ctx, p360_db_host, p360_db_port, p360_db_name,
                   p360_db_user, p360_db_password, download_directory):

    """
    Load data from csv to p360.
    """
    cpq_api = ctx.obj['api']
    p360_engine = create_postgresql_engine(
        host=p360_db_host,
        port=p360_db_port,
        name=p360_db_name,
        user=p360_db_user,
        password=p360_db_password,
    )

    log.info(download_directory)
    list_of_files = cpq_api.extract_zip(download_directory)
    log.info(f'List of files {list_of_files}')
    read_load_file_impl(cpq_api, p360_engine, list_of_files, download_directory)

def read_load_file_impl(cpq_api, p360_engine, list_csv_files, download_directory):

    cpy_csv_to_tble(cpq_api, list_csv_files, p360_engine, download_directory)

    p360_engine.close

# function to create  table
def create_attR_table(cpq_api, p360_engine, list_of_files, download_directory):
    #cur_p360 = p360_engine.cursor()

    # table name to load the data
    table_name = "p360.temp_attrTable"

    size_of_list = len(list_of_files)
    print("total table count is {}".format(size_of_list))

    # read first csv file
    csv_file = path.join(download_directory, list_of_files[0])
    log.info("Reading csv file {} in dataframe.".format(csv_file))
    csv_file_df = pd.read_csv(csv_file)

    # fetch the first header
    table_cols =  csv_file_df.iloc[[0]]

    # fetch the second header
    cols_data_type = csv_file_df.iloc[[1]]

    # collect the tables column names in the list data structure
    table_cols = table_cols.to_string().split()
    cols_data_type = cols_data_type.to_string().split()

    # creating dict{} with cols, data type, null
    data = { 'table_cols' : table_cols,
         'cols_data_type' : cols_data_type,
         'null' : 'NULL'
         }

    # creating dataframe of cols, datatype, null
    df = pd.DataFrame(data)

    # list of values
    df_list = df.values.tolist()

    # removing first three words with slicing
    del df_list[0:3]

    col_size = df.shape[0] - 3
    print("CSV having columns {}".format(col_size))

    # creating list of tuples from list of list
    list_of_tuples = [ tuple(i) for i in df_list ]

    # creating list of tuples[ (col dataype null), (col, datatype null) ...]
    sql_string = list(map(cpq_api.join_elements, list_of_tuples))

    string = " ".join(sql_string)
    string = string.replace('String', 'VARCHAR')
    string1 = string[:-1]

    # dropping the table
    log.info("Dropping table -- {}".format(table_name))

    # Drop table If exists
    DEL_SQL = "DROP TABLE if exists " + table_name
    result = p360_engine.execute(DEL_SQL)
    log.info("table dropped.")

    log.info("\nCreating table -- {} with {} columns.".format(table_name, col_size))
    print("\nCreating table -- {} with {} columns.".format(table_name, col_size))

    SQL = "CREATE TABLE IF NOT EXISTS " + table_name + "(" + string1 + ")"
    result = p360_engine.execute(SQL)
    log.info("Table {} created.".format(table_name))
    print("Table {} created.".format(table_name))

    return table_name, col_size

def cpy_csv_to_tble(cpq_api, list_of_files, p360_engine, download_directory):

    cursor = p360_engine.cursor()

    # creating table
    attR_table, column_size = create_attR_table(cpq_api, p360_engine, list_of_files, download_directory)

    for csv_file in list_of_files:

        log.info(f"Processing data loading process for {csv_file}")
        csv_file = path.join(download_directory, csv_file)

        csv_df = pd.read_csv(csv_file)
        table_cols = csv_df.iloc[[0]]
        table_cols = table_cols.to_string().split()
        del table_cols[0:3]

        # get the size of columns
        local_col_size = len(table_cols)
        log.info(f"File {csv_file} is having column size {local_col_size}")

        s = open(csv_file, 'r').read()
        # to escape characters
        # chars = ('$','%','^','*','\\') # etc
        chars = ('\\')  # etc
        for c in chars:
            s = '_'.join(s.split(c))

        # logic to make csv having less columns , compatible with other csv's
        if column_size != local_col_size:

            log.info("difference found in columns size.")

            get_col_diff = column_size - local_col_size
            delimiter_str = "," * get_col_diff
            delimiter_str = delimiter_str + "\n"
            # print(delimiter_str)

            log.info("adding {} columns in the file {}".format(get_col_diff, csvFile))
            s = s.replace('\n', delimiter_str)
            #s = s.replace('\n', ',,\n')
            s = s.rstrip("\n")

        out_file = open(csv_file, 'w')
        out_file.write(s)
        out_file.close()

        with open(csv_file, 'r') as f:
            next(f) # Skip the header row.
            next(f)
            next(f)
            next(f)
            next(f)
            cursor.copy_from(f, attR_table, sep=',', null='None')

        p360_engine.commit()

# get the end time
end_time = time.time()

# get the processed time
#time_interval =  end_time - start_time

#print("\nLoading process completed.")
#print("Loading data process has been Completed at {}".format(end_time))
#print("Time required to load the data is => {}".format(time_interval))




