from ast import literal_eval
from botocore.exceptions import ClientError

def getSecretString(secretsmanager):
    print('getSecretString')
    try:
        resp = secretsmanager.get_secret_value(SecretId='fitbit/token')
    except ClientError as e:
        raise e
    else:
        secret_string = resp['SecretString']
        print(secret_string)
    return secret_string

def write_records(timestream, records):
    try:
        result = timestream.write_records(
            DatabaseName='bakubaku',
            TableName='fitbit',
            Records=records,
            CommonAttributes={}
        )
        status = result['ResponseMetadata']['HTTPStatusCode']
        print("Processed %d records.WriteRecords Status: %s" % (len(records), status))
    except timestream.exceptions.RejectedRecordsException as err:
        print("Timestream RejectedRecordsException: ", err)
    except Exception as err:
        print("Timestream Exception: ", err)
