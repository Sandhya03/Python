import os

import boto3
from pyspark import SparkContext
from pyspark.sql import SparkSession
from datetime import date

import argparse
import logging

import findspark
findspark.init()

parser = argparse.ArgumentParser(description='read CSV file from s3')
parser.add_argument('--file', required=True, help='csv file name to download from s3 bucket.')
parser.add_argument('--column', help='data will get displayed for provided column.')
parser.add_argument('--filetype', help='default file type is csv.')
args = vars(parser.parse_args())

logging.basicConfig(filename='csv_conn_log.log', filemode='w', level=logging.INFO,
                    format='%(name)s - %(levelname)s - %(message)s')
logging.warning('This will get logged to a file')

os.environ['AWS_KEY_ID'] = ''
os.environ['AWS_SECRET'] = ''

AWS_KEY_ID = os.environ['AWS_KEY_ID']
AWS_SECRET = os.environ['AWS_SECRET']

bucket_name = 'bhcsvbucketest'

file_name = args['file']
file_type = args['filetype']

# get current date
today = date.today()

def chk_file_valid(file, client):
    logging.info("checking file is valid or not --")

    res = client.head_object(Bucket=bucket_name, Key=file_name)
    file_size = res['ContentLength']
    logging.info(f"Size of file {file_name} is {file_size}i bytes.")
    print(f"Size of file {file_name} is {file_size}")

    if file_size < 3:
        logging.info("File having no data.")
        exit()

    file_type = file.lower().endswith(('.csv', '.txt'))
    if not file_type:
        print("File type is not valid.")
        exit()

    return file

# creating s3 client
client = boto3.client('s3', region_name='us-east-1', aws_access_key_id=AWS_KEY_ID,
                      aws_secret_access_key=AWS_SECRET
                      )

try:
    # list bucket objects
    response = client.list_objects(
        Bucket=bucket_name,
    )
except Exception as msg:
    logging.exception(msg)
    print("The specified bucket does not exist")
    exit()


file_list = [item['Key'] for item in response['Contents']]
logging.info(file_list)
print(f"exisiting objects in bucket {bucket_name} : {file_list}")

if file_name not in file_list:
    logging.exception("File not found in bucket")
    exit()



column_name = args['column']
logging.info(f'file {file_name} column {column_name}')

# Validating file
file = chk_file_valid(file_name, client)

# creating SparkContext
sc = SparkContext.getOrCreate()

# creating a SparkSession
spark = SparkSession.builder.master("local[*]").appName('Pyspark Dataframe').getOrCreate()

spark._jsc.hadoopConfiguration().set('fs.s3.impl', 'org.apache.hadoop.fs.s3native.NativeS3FileSystem')
spark._jsc.hadoopConfiguration().set('fs.s3.awsAccessKeyId', AWS_KEY_ID)
spark._jsc.hadoopConfiguration().set('fs.s3.awsSecretAccessKey', AWS_SECRET)
spark._jsc.hadoopConfiguration().set('fs.s3.buffer.dir', 'file:///tmp')

print(f"Reading file {file} from s3 bucket.")
logging.info(f"Reading file {file} from s3 bucket.")

# reading file from s3
df = spark.read.csv(f"s3://bhcsvbucketest/{file}", header=True, inferSchema=True)

if len(df.columns) == 0:
    print(f"file {file} is of 0 bytes.")
    logging.info(f"file {file} is of 0 bytes.")

if column_name:
    df = df.select(column_name)

f1 = file.split('.')

# new file name
out_file = f"out_{f1[0]}"

print(f"Saving dataframe in s3 as name {out_file}")
df.show(3)

if args['filetype'] == 'parquet':
    logging.info(f"writing dataframe in parquet format in s3 bucketi as filename {out_file}")
    print(f"writing dataframe in parquet format in s3 bucket as filename {out_file}")

    try:
        # saving dataframe as parquet
        print(f"writing dataframe in parquet format in s3 bucket as filename {out_file}")
        #df.write.parquet(f"s3://bhcsvbucketest/{out_file}.parquet", mode="overwrite")
        df.write.parquet(f"{out_file}.parquet", mode="overwrite")
        #df.select(column_name).write.save(f"s3://bhcsvbucketest/{out_file}.parquet", format="parquet")
    except Exception as e:
        print(e)
        logging.debug(e)
else:
    logging.info("writing dataframe to csv in s3")
    print("writing dataframe to csv in s3")
    # saving file as csv
    #df.select(column_name).write.save(f"s3://bhcsvbucketest/{out_file}.csv", format="csv")

    try:

        #df.coalesce(1).write.option("header", "true").format("csv").save("s3://bhcsvbucketest/test1")
        #df.write.option("header", "true").csv(f"s3://bhcsvbucketest/{out_file}")
        df.write.option("header", "true").csv(f"s3://bhcsvbucketest/testing1")
        #df.write.option("header", "true").csv("testing1")
        #df.write.options(header='True', delimiter=',').csv("test1")
    except Exception as e:
        print(e)
        logging.info(e)

count = df.count()
logging.info(f"Total records {count}")
