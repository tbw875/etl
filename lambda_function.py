import requests
import json
import boto3
from datetime import date, datetime


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
    bucket = "seattle-parking-data"
    file_name = f"parking_data_{date.today().isoformat()}.json"

    # Upload to S3
    s3.put_object(Body=data_json, Bucket=bucket, Key=file_name)

    return {
        "statusCode": 200,
        "body": json.dumps(
            f"Parking data for {date.today().isoformat()} successfully uploaded to {bucket}!"
        ),
    }
