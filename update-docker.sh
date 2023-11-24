#!/bin/bash

# Define variables
IMAGE_NAME="seattle-parking-image"
ECR_REPO_URI="799386638629.dkr.ecr.us-west-2.amazonaws.com/seattle-parking"
LAMBDA_FUNCTION_NAME="seattle-parking-extract"
REGION="us-west-2"

# Step 1: Build the Docker image
echo "Building Docker image..."
docker build -t $IMAGE_NAME:latest .

# Step 2: Authenticate Docker to your ECR registry
echo "Authenticating to ECR..."
aws ecr get-login-password --region $REGION | docker login --username AWS --password-stdin $ECR_REPO_URI

# Step 3: Tag and push the image to ECR
echo "Tagging and pushing the image to ECR..."
docker tag $IMAGE_NAME:latest $ECR_REPO_URI:latest
docker push $ECR_REPO_URI:latest

# Step 4: Update the Lambda function
echo "Updating Lambda function..."
aws lambda update-function-code --function-name $LAMBDA_FUNCTION_NAME --image-uri $ECR_REPO_URI:latest --region $REGION

echo "Update complete!"
