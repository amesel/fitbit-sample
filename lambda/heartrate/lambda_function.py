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
secretsmanager = session.client(
    service_name = 'secretsmanager',
    region_name = REGION
)
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
    data = client.intraday_time_series(resource,
        base_date='today',
        detail_level='15min',
        start_time='00:00',
        end_time='23:59',
    )
    return data

def write_hr(dataset):
    records = []
    now = datetime.now(JST)
    for record in dataset:
        h, m, nil = record['time'].split(':')
        t = now.replace(hour=int(h), minute=int(m), second=0)
        epoch_time = int(t.timestamp())
        value = record['value']
        if value == 0:
            continue
        records.append ({
            'Time': str(epoch_time),
            'TimeUnit': 'SECONDS',
            'Dimensions': [ {'Name': 'user_id', 'Value': USER_ID} ],
            'MeasureName': 'heartrate',
            'MeasureValue': str(value),
            'MeasureValueType': 'BIGINT'
        })
    print(records)
    common.write_records(timestream, records)

def lambda_handler(event, context):
    client = get_fitbit_client()

    # hr
    data = get_dataset(client, 'activities/heart')
    write_hr(data["activities-heart-intraday"]["dataset"])
