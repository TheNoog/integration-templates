#!/usr/bin/env python

# https://medium.com/@juliorenteria/upload-csv-to-dynamodb-using-python-dfe150ed8f35

import pandas as pd
import os
import boto3
pd.set_option('display.max_columns', None)
import json
from decimal import Decimal
from tqdm import tqdm

# Get credentials
DEFAULT_LOCATION="~/.aws/credentials"
DATA = pd.read_csv(DEFAULT_LOCATION)
AWS_ACCESS_KEY_ID=str(DATA.iloc[0][0]).split(' ')[2]
AWS_SECRET_ACCESS_KEY=str(DATA.iloc[1][0]).split(' ')[2]
REGION="ap-southeast-2"

# Collect CSV and convert to JSON
CSV_NAME="waste_recycling"
DATA_DIR = "/".join(os.getcwd().split('/')[1:6]) + '/data'
df = json.loads(pd.read_csv(f"/{DATA_DIR}/{CSV_NAME}.csv").to_json(orient='records'), parse_float=Decimal)
# Create a list of Dictionaries and their table name.
lst_Dics = [{'item': df, 'table':f'{CSV_NAME}'}]

# Connect to DynamoDb Function
dynamodb = boto3.resource('dynamodb', \
        aws_access_key_id=AWS_ACCESS_KEY_ID, \
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY, \
            region_name=REGION)

def insertDynamoItem (tablename,item_lst):
    dynamoTable = dynamodb.Table(tablename)
    
    print("Starting upload...")

    for record in tqdm(item_lst):
        dynamoTable.put_item(Item=record)
    
    print('Success')


for element in lst_Dics:
    insertDynamoItem(element['table'],element['item'])

# For this to work you must have previously created the tables in DynamoDB 
# and the Primary key declared when creating them must be present in the 
# file to upload, and must be the same type as declared (number, string, bool).


# if __name__ == '__main__':
#     main()