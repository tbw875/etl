# Seattle Parking Data ETL Project

This project is an ETL pipeline that extracts parking data from the City of Seattle's Open Data API, transforms it, and loads it into an AWS S3 bucket. The data is then intended to be loaded into a relational database on AWS for further analysis. This project showcases the use of AWS Lambda, Docker, and other AWS services to create an efficient and scalable data processing pipeline.

## Description

The Seattle Parking Data ETL Project is designed to handle large volumes of parking transaction data. It uses AWS Lambda for processing, ensuring scalability and cost-effectiveness. The data, once transformed and stored in S3, can be used for various analytical purposes, such as urban planning, parking pattern analysis, or as a dataset for machine learning models.

## Getting Started

### Dependencies

- Python 3.x
- AWS CLI
- Docker
- Boto3
- Requests library (Python)

### Installing

1. Clone the repository to your local machine.
2. Ensure you have AWS CLI installed and configured with your AWS account.
3. Install Docker on your machine.

### Executing program

To run the ETL pipeline:

1. Navigate to the project directory.
2. Build the Docker image:
```bash
docker build -t seattle-parking-etl:latest .
```
3. Run the AWS commands to push the image to ECR and update the Lambda function (refer to the automated script section).

### Automated Script

For ease of deployment, an automated script `update-lambda.sh` is provided to build the Docker image, push it to AWS ECR, and update the Lambda function:

```bash
./update-lambda.sh
```

### AWS Lambda Function

The AWS Lambda function (lambda_function.py) is triggered to execute the ETL process, which involves:

1. Extracting data from the City of Seattle's Open Data API.
2. Transforming the datetime format.
3. Uploading the transformed data to an S3 bucket.

### Docker Configuration

Docker is used to containerize the Lambda function. The Dockerfile is provided to build the container image, which is then pushed to AWS ECR.

---

## AWS Services Used

* AWS Lambda: Runs the ETL script
* Amazon S3: Stores the transformed data
* Amazon ECR: Hosts the Docker container image

### Contributing

If you wish to contribute to this project, please fork the repository and issue a pull request.

### Version History

* v0.1 - Initial Release

### To Do

* Load the transformed data into a relational database
* Use the relational database to analyze insights
* Visualize insights on a purpose-built visualization platform (e.g. Tableau) or build a bespoke front-end

### Authors

* Tom Walsh
* Contact: [dm via github]

### License

This project is licensed under the MIT License - see the LICENSE.md file for details

### Acknowledgements
* Thank you to the City of Seattle for providing the transparent city data via API
* Please drive carefully -- 20 is better than 30!