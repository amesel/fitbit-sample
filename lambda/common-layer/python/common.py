import boto3
import fitbit
import os
from ast import literal_eval
from botocore.config import Config
from botocore.exceptions import ClientError

REGION = "ap-northeast-1"
CLIENT_ID = os.environ["CLIENT_ID"]
CLIENT_SECRET = os.environ["CLIENT_SECRET"]
SECRET_ID = "fitbit/token"
DATABASE_NAME = "bakubaku"
TABLE_NAME = "fitbit"


class Common:
    def __init__(self):
        session = boto3.session.Session()
        self.secretsmanager = session.client(
            service_name="secretsmanager", region_name=REGION
        )
        self.timestream = session.client(
            service_name="timestream-write",
            region_name=REGION,
            config=Config(
                read_timeout=20, max_pool_connections=5000, retries={"max_attempts": 10}
            ),
        )

    def get_token(self):
        try:
            resp = self.secretsmanager.get_secret_value(SecretId=SECRET_ID)
        except ClientError as e:
            raise e
        else:
            token = resp["SecretString"]
            print(token)
            return token

    def update_token(self, token):
        print("updateToken")
        token_str = str(token)
        print(token_str)
        self.secretsmanager.put_secret_value(
            SecretId=SECRET_ID,
            SecretString=token_str,
        )
        return

    def get_fitbit_client(self):
        token_dict = literal_eval(self.get_token())
        access_token = token_dict["access_token"]
        refresh_token = token_dict["refresh_token"]
        print(access_token)
        print(refresh_token)
        client = fitbit.Fitbit(
            CLIENT_ID,
            CLIENT_SECRET,
            access_token=access_token,
            refresh_token=refresh_token,
            refresh_cb=self.update_token,
        )
        return client

    def write_ts_records(self, records):
        try:
            result = self.timestream.write_records(
                DatabaseName=DATABASE_NAME,
                TableName=TABLE_NAME,
                Records=records,
                CommonAttributes={},
            )
            status = result["ResponseMetadata"]["HTTPStatusCode"]
            print(
                "Processed %d records.WriteRecords Status: %s" % (len(records), status)
            )
        except self.timestream.exceptions.RejectedRecordsException as err:
            print("Timestream RejectedRecordsException: ", err)
        except Exception as err:
            print("Timestream Exception: ", err)
