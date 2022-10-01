from datetime import datetime, timedelta, timezone
import common

USER_ID = "7JT7MH"
JST = timezone(timedelta(hours=+9), "JST")


def get_fitbit_data(client, resource):
    now = datetime.now()
    past = now + timedelta(days=-14)
    start_day = past.strftime("%Y-%m-%d")
    end_day = now.strftime("%Y-%m-%d")
    data = client.time_series(
        resource,
        base_date=start_day,
        end_date=end_day,
    )
    return data


def make_br_records(dataset):
    records = []
    for record in dataset:
        dt = datetime.strptime(record["dateTime"], "%Y-%m-%d")
        epoch_time = int(dt.timestamp())
        value = record["value"]["breathingRate"]
        if value == 0:
            continue
        records.append(
            {
                "Time": str(epoch_time),
                "TimeUnit": "SECONDS",
                "Dimensions": [{"Name": "user_id", "Value": USER_ID}],
                "MeasureName": "br",
                "MeasureValue": str(value),
                "MeasureValueType": "DOUBLE",
            }
        )
    print(records)
    return records


def make_tempskin_records(dataset):
    records = []
    for record in dataset:
        dt = datetime.strptime(record["dateTime"], "%Y-%m-%d")
        epoch_time = int(dt.timestamp())
        value = record["value"]["nightlyRelative"]
        if value == 0:
            continue
        records.append(
            {
                "Time": str(epoch_time),
                "TimeUnit": "SECONDS",
                "Dimensions": [{"Name": "user_id", "Value": USER_ID}],
                "MeasureName": "tempskin",
                "MeasureValue": str(value),
                "MeasureValueType": "DOUBLE",
            }
        )
    print(records)
    return records


def make_hrv_records(dataset):
    records = []
    for record in dataset:
        dt = datetime.strptime(record["dateTime"], "%Y-%m-%d")
        epoch_time = int(dt.timestamp())
        for key in record["value"]:
            value = record["value"][key]
            if value == 0:
                continue
            records.append(
                {
                    "Time": str(epoch_time),
                    "TimeUnit": "SECONDS",
                    "Dimensions": [{"Name": "user_id", "Value": USER_ID}],
                    "MeasureName": key,
                    "MeasureValue": str(value),
                    "MeasureValueType": "DOUBLE",
                }
            )
    print(records)
    return records


def make_spo2_records(dataset):
    records = []
    for record in dataset:
        dt = datetime.strptime(record["dateTime"], "%Y-%m-%d")
        epoch_time = int(dt.timestamp())
        for key in record["value"]:
            value = record["value"][key]
            if value == 0:
                continue
            records.append(
                {
                    "Time": str(epoch_time),
                    "TimeUnit": "SECONDS",
                    "Dimensions": [{"Name": "user_id", "Value": USER_ID}],
                    "MeasureName": "spo2_" + key,
                    "MeasureValue": str(value),
                    "MeasureValueType": "DOUBLE",
                }
            )
    print(records)
    return records


def lambda_handler(event, context):
    util = common.Common()
    client = util.get_fitbit_client()
    # br
    data = get_fitbit_data(client, "br")
    util.write_ts_records(make_br_records(data["br"]))
    # tempskin
    data = get_fitbit_data(client, "temp/skin")
    util.write_ts_records(make_tempskin_records(data["tempSkin"]))
    # hrv
    data = get_fitbit_data(client, "hrv")
    util.write_ts_records(make_hrv_records(data["hrv"]))
    # spo2
    data = get_fitbit_data(client, "spo2")
    util.write_ts_records(make_spo2_records(data))
