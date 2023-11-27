import requests
import json
import boto3
from datetime import date, datetime
import pymysql
import os


# New: Function to load data from S3 into RDS
def load_data_into_rds(file_name, bucket_name):
    # Database connection parameters
    rds_host = "seattle-paid-parking.ckhfrrg1sdtj.us-west-2.rds.amazonaws.com"
    user = os.environ["RDS_USERNAME"]
    password = os.environ["RDS_PASSWORD"]
    db_name = "seattle-paid-parking"

    # Connect to the database
    conn = pymysql.connect(
        host=rds_host, user=user, passwd=password, db=db_name, connect_timeout=5
    )

    with conn.cursor() as cur:
        load_query = f"""
        LOAD DATA FROM S3 's3://{bucket_name}/{file_name}'
        INTO TABLE paid_parking
        FIELDS TERMINATED BY ','
        LINES TERMINATED BY '\\n'
        (transaction_id, meter_code, transactiondatetime, payment_mean, amount_paid, durationinminutes, blockface_name, sideofstreet, elementkey, parkingspacenumber, latitude, longitude);
        """
        cur.execute(load_query)
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
    # Fetch the data from the Seattle Open Data Portal
    PARKING_ENDPOINT = "https://data.seattle.gov/resource/gg89-k5p6.json?$query=SELECT%20transaction_id%2C%20meter_code%2C%20transactiondatetime%2C%20payment_mean%2C%20amount_paid%2C%20durationinminutes%2C%20blockface_name%2C%20sideofstreet%2C%20elementkey%2C%20parkingspacenumber%2C%20latitude%2C%20longitude%20ORDER%20BY%20%3Aid%20ASC%20LIMIT%201000000"
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
