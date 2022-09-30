import os
import boto3
import fitbit
import time
from datetime import datetime, timedelta, timezone
from ast import literal_eval
from botocore.exceptions import ClientError
from botocore.config import Config
import common

CLIENT_ID = os.environ['CLIENT_ID']
CLIENT_SECRET = os.environ['CLIENT_SECRET']
REGION = "ap-northeast-1"
USER_ID = "7JT7MH"
TS_DATABASE = "bakubaku"
TS_TABLE = "fitbit"
SECRET_ID = "fitbit/token"
JST = timezone(timedelta(hours=+9), 'JST')

secret = {}

session = boto3.session.Session()
# secretsmanager client
secretsmanager = session.client(
    service_name = 'secretsmanager',
    region_name = REGION
)
# timestream client
timestream = session.client(
    service_name = 'timestream-write',
    region_name = REGION,
    config = Config(
        read_timeout=20, 
        max_pool_connections=5000, 
        retries={'max_attempts': 10}
    )
)

def getToken():
    global secret
    print('getToken')
    if len(secret) != 0 and time.time() < literal_eval(secret)['expires_at']:
        print('in-memory: ' + secret)
        return secret
    else:
        secret = common.getSecretString(secretsmanager)
        print('secretsmanager: ' + secret)
        return secret

def updateToken(token):
    print('updateToken')
    token_str = str(token)
    print(token_str)
    secretsmanager.put_secret_value(
        SecretId=SECRET_ID,
        SecretString=token_str,
    )
    return

def get_fitbit_client():
    token_dict = literal_eval(getToken())
    access_token = token_dict['access_token']
    refresh_token = token_dict['refresh_token']
    # print(access_token)
    # print(refresh_token)
    client = fitbit.Fitbit(
        CLIENT_ID, 
        CLIENT_SECRET,
        access_token = access_token, 
        refresh_token = refresh_token, 
        refresh_cb = updateToken
    )
    return client

def get_dataset(client, resource):
    now = datetime.now()
    past = now + timedelta(days=-14)
    start_day = past.strftime('%Y-%m-%d')
    end_day = now.strftime('%Y-%m-%d')
    data = client.time_series(resource,
        base_date=start_day,
        end_date=end_day,
    )
    return data

def write_br(dataset):
    records = []
    for record in dataset:
        dt = datetime.strptime(record['dateTime'], '%Y-%m-%d')
        epoch_time = int(dt.timestamp())
        value = record['value']['breathingRate']
        if value == 0:
            continue
        records.append ({
            'Time': str(epoch_time),
            'TimeUnit': 'SECONDS',
            'Dimensions': [ {'Name': 'user_id', 'Value': USER_ID} ],
            'MeasureName': 'br',
            'MeasureValue': str(value),
            'MeasureValueType': 'DOUBLE'
        })
    print(records)
    common.write_records(timestream, records)

def write_tempskin(dataset):
    records = []
    for record in dataset:
        dt = datetime.strptime(record['dateTime'], '%Y-%m-%d')
        epoch_time = int(dt.timestamp())
        value = record['value']['nightlyRelative']
        if value == 0:
            continue
        records.append ({
            'Time': str(epoch_time),
            'TimeUnit': 'SECONDS',
            'Dimensions': [ {'Name': 'user_id', 'Value': USER_ID} ],
            'MeasureName': 'tempskin',
            'MeasureValue': str(value),
            'MeasureValueType': 'DOUBLE'
        })
    print(records)
    common.write_records(timestream, records)

def write_hrv(dataset):
    records = []
    for record in dataset:
        dt = datetime.strptime(record['dateTime'], '%Y-%m-%d')
        epoch_time = int(dt.timestamp())
        for key in record['value']:
            value = record['value'][key]
            if value == 0:
                continue
            records.append ({
                'Time': str(epoch_time),
                'TimeUnit': 'SECONDS',
                'Dimensions': [ {'Name': 'user_id', 'Value': USER_ID} ],
                'MeasureName': key,
                'MeasureValue': str(value),
                'MeasureValueType': 'DOUBLE'
            })
    print(records)
    common.write_records(timestream, records)

def write_spo2(dataset):
    records = []
    for record in dataset:
        dt = datetime.strptime(record['dateTime'], '%Y-%m-%d')
        epoch_time = int(dt.timestamp())
        for key in record['value']:
            value = record['value'][key]
            if value == 0:
                continue
            records.append ({
                'Time': str(epoch_time),
                'TimeUnit': 'SECONDS',
                'Dimensions': [ {'Name': 'user_id', 'Value': USER_ID} ],
                'MeasureName': 'spo2_' + key,
                'MeasureValue': str(value),
                'MeasureValueType': 'DOUBLE'
            })
    print(records)
    common.write_records(timestream, records)

def lambda_handler(event, context):
    client = get_fitbit_client()
    # br
    data = get_dataset(client, 'br')
    write_br(data['br'])
    # tempskin
    data = get_dataset(client, 'temp/skin')
    write_tempskin(data['tempSkin'])
    # hrv
    data = get_dataset(client, 'hrv')
    write_hrv(data['hrv'])
    # spo2
    data = get_dataset(client, 'spo2')
    write_spo2(data)
