import requests
import json
import boto3
from datetime import date, datetime, timedelta
import pymysql
import os
import logging

logger = logging.getLogger()
logger.setLevel(logging.INFO)


def load_data_into_rds(file_name, bucket_name):
    rds_host = "seattle-paid-parking.ckhfrrg1sdtj.us-west-2.rds.amazonaws.com"
    user = os.environ["RDS_USERNAME"]
    password = os.environ["RDS_PASSWORD"]
    db_name = "seattle-paid-parking"

    logging.info("Connecting to RDS")
    try:
        conn = pymysql.connect(
            host=rds_host, user=user, passwd=password, db=db_name, connect_timeout=120
        )
        logging.info("Connected to RDS")
    except pymysql.MySQLError as e:
        logging.error(
            f"ERROR: Unexpected error: Could not connect to MySQL instance. {e}"
        )
        return

    logging.info("Reading JSON data from S3")
    s3 = boto3.client("s3")
    try:
        obj = s3.get_object(Bucket=bucket_name, Key=file_name)
        json_data = json.loads(obj["Body"].read().decode("utf-8"))
    except Exception as e:
        logging.error(f"ERROR: Could not read JSON data from S3. {e}")
        return

    insert_stmt = """
    INSERT INTO paid_parking
    (transaction_id, meter_code, transactiondatetime, payment_mean, amount_paid, durationinminutes, blockface_name, sideofstreet, elementkey, latitude, longitude)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """

    try:
        with conn.cursor() as cur:
            for record in json_data:
                cur.execute(
                    insert_stmt,
                    (
                        record.get("transaction_id"),
                        record.get("meter_code"),
                        record.get("transactiondatetime"),
                        record.get("payment_mean"),
                        record.get("amount_paid"),
                        record.get("durationinminutes"),
                        record.get("blockface_name"),
                        record.get("sideofstreet"),
                        record.get("elementkey"),
                        record.get("latitude"),
                        record.get("longitude"),
                    ),
                )
            conn.commit()
            logging.info("Data successfully inserted into RDS")
    except pymysql.MySQLError as e:
        logging.error(f"ERROR: Failed to insert data into MySQL table. {e}")
    finally:
        conn.close()
        logging.info("Connection to RDS closed")


def handler(event, context):
    logger.info("Starting Handler")

    yesterday = datetime.now() - timedelta(days=1)
    formatted_yesterday = yesterday.strftime("%Y-%m-%d")

    logger.info("Fetching data from Seattle Open Data Portal API")
    PARKING_ENDPOINT = f"https://data.seattle.gov/resource/gg89-k5p6.json?$where=transactiondatetime>='{formatted_yesterday}'&$$app_token={os.environ['APP_TOKEN']}"
    try:
        response = requests.get(PARKING_ENDPOINT, timeout=120)
        response.raise_for_status()
        data = response.json()
    except requests.RequestException as e:
        logger.error(f"ERROR: Failed to fetch data from API. {e}")
        return {
            "statusCode": 500,
            "body": json.dumps(f"Failed to fetch data from API. {e}"),
        }

    logger.info("Test endpoint")
    TEST_ENDPOINT = "https://httpbin.org/get"
    try:
        response = requests.get(TEST_ENDPOINT, timeout=120)
        if response.status_code == 200:
            logger.info("Test endpoint returned status: 200")
            return {
                "statusCode": 200,
                "body": json.dumps("Test endpoint returned status: 200"),
            }
        else:
            logger.error(f"Test endpoint returned status: {response.status_code}")
            return {
                "statusCode": response.status_code,
                "body": json.dumps(
                    f"Test endpoint returned status: {response.status_code}"
                ),
            }
    except requests.RequestException as e:
        logger.error(f"ERROR: Failed to reach test endpoint. {e}")
        return {
            "statusCode": 500,
            "body": json.dumps(f"Failed to reach test endpoint. {e}"),
        }

    data_json = json.dumps(data)

    s3 = boto3.resource("s3")
    logging.info("Connected to S3")

    bucket_name = "seattle-parking-data"
    file_name = f"parking_data_{date.today().isoformat()}.json"

    try:
        s3.Object(bucket_name, file_name).put(Body=data_json)
        logging.info(f"Uploaded {file_name} to {bucket_name}")
    except Exception as e:
        logging.error(f"ERROR: Failed to upload data to S3. {e}")
        return {
            "statusCode": 500,
            "body": json.dumps(f"Failed to upload data to S3. {e}"),
        }

    load_data_into_rds(file_name, bucket_name)

    return {
        "statusCode": 200,
        "body": json.dumps(
            f"Parking data for {date.today().isoformat()} successfully uploaded to {bucket_name} and loaded into RDS!"
        ),
    }
