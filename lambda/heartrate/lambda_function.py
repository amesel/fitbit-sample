from datetime import datetime, timedelta, timezone
import common

USER_ID = "7JT7MH"
JST = timezone(timedelta(hours=+9), "JST")


def get_fitbit_data(client, resource):
    data = client.intraday_time_series(
        resource,
        base_date="today",
        detail_level="15min",
        start_time="00:00",
        end_time="23:59",
    )
    return data


def make_hr_records(dataset):
    records = []
    now = datetime.now(JST)
    for record in dataset:
        h, m, nil = record["time"].split(":")
        t = now.replace(hour=int(h), minute=int(m), second=0)
        epoch_time = int(t.timestamp())
        value = record["value"]
        if value == 0:
            continue
        records.append(
            {
                "Time": str(epoch_time),
                "TimeUnit": "SECONDS",
                "Dimensions": [{"Name": "user_id", "Value": USER_ID}],
                "MeasureName": "heartrate",
                "MeasureValue": str(value),
                "MeasureValueType": "BIGINT",
            }
        )
    print(records)
    return records


def lambda_handler(event, context):
    util = common.Common()
    client = util.get_fitbit_client()
    data = get_fitbit_data(client, "activities/heart")
    records = make_hr_records(data["activities-heart-intraday"]["dataset"])
    util.write_ts_records(records)
