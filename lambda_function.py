import requests
import json
import boto3
from datetime import date, datetime
import pymysql
import os
from datetime import datetime, timedelta


def load_data_into_rds(file_name, bucket_name):
    # Database connection parameters
    rds_host = "seattle-paid-parking.ckhfrrg1sdtj.us-west-2.rds.amazonaws.com"
    user = os.environ["RDS_USERNAME"]
    password = os.environ["RDS_PASSWORD"]
    db_name = "seattle-paid-parking"

    # Connect to the database
    conn = pymysql.connect(
        host=rds_host, user=user, passwd=password, db=db_name, connect_timeout=120
    )

    # Read JSON data from S3
    s3 = boto3.client("s3")
    obj = s3.get_object(Bucket=bucket_name, Key=file_name)
    json_data = json.loads(obj["Body"].read().decode("utf-8"))

    # SQL Insert Statement
    insert_stmt = """
    INSERT INTO paid_parking
    (transaction_id, meter_code, transactiondatetime, payment_mean, amount_paid, durationinminutes, blockface_name, sideofstreet, elementkey, latitude, longitude)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """

    # Insert data into the database
    with conn.cursor() as cur:
        for record in json_data:
            cur.execute(
                insert_stmt,
                (
                    record["transaction_id"],
                    record["meter_code"],
                    record["transactiondatetime"],
                    record["payment_mean"],
                    record["amount_paid"],
                    record["durationinminutes"],
                    record["blockface_name"],
                    record["sideofstreet"],
                    record["elementkey"],
                    record["latitude"],
                    record["longitude"],
                ),
            )
        conn.commit()

    conn.close()


def transform_data(data):
    # Transformation of datetime format during extraction
    for record in data:
        if "transactiondatetime" in record:
            record["transactiondatetime"] = datetime.strptime(
                record["transactiondatetime"], "%Y-%m-%dT%H:%M:%S.%f"
            ).strftime("%Y-%m-%d %H:%M:%S")
    return data


def handler(event, context):
    # Calculate yesterday's date
    yesterday = datetime.now() - timedelta(days=1)
    formatted_yesterday = yesterday.strftime("%Y-%m-%d")

    # Fetch the data from the Seattle Open Data Portal
    PARKING_ENDPOINT = f"https://data.seattle.gov/resource/gg89-k5p6.json?$where=transactiondatetime>='{formatted_yesterday}'&$$app_token={os.environ['APP_TOKEN']}"
    response = requests.get(PARKING_ENDPOINT)
    data = response.json()

    # Transform the data
    data = transform_data(data)

    # Convert to JSON
    data_json = json.dumps(data)

    # Set up s3 controller
    s3 = boto3.resource("s3")

    # Define bucket name and file name
    bucket_name = "seattle-parking-data"
    file_name = f"parking_data_{date.today().isoformat()}.json"

    # Upload to S3
    s3.Object(bucket_name, file_name).put(Body=data_json)

    load_data_into_rds(file_name, bucket_name)

    return {
        "statusCode": 200,
        "body": json.dumps(
            f"Parking data for {date.today().isoformat()} successfully uploaded to {bucket_name} and loaded into RDS!"
        ),
    }
