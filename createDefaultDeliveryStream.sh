#!/bin/bash

# Kinesis Streams to Firehose
# 
# Copyright 2015 Amazon.com, Inc. or its affiliates. All Rights Reserved.
# 
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# 
#     http://www.apache.org/licenses/LICENSE-2.0
# 
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


s3BucketNamePrefix="LambdaStreamsDefaultDeliveryBucket"
iamRoleName="LambdaStreamsDefaultDeliveryRole"
iamPolicyName="LambdaStreamsDefaultDeliveryPolicy"
deliveryStreamName="LambdaStreamsDefaultDeliveryStream"

which aws > /dev/null 2>&1

if [ $? != 0 ]; then
	echo "This utility requires the AWS Cli, which can be installed using instructions found at http://docs.aws.amazon.com/cli/latest/userguide/installing.html"
	exit -2
fi

region=us-east-1

if [ $# != 1 ]; then
  echo "Proceeding with default region us-east-1"
else
  region=$1
fi

randomChars=$(openssl rand -base64 8 | tr -dc 'a-cA-Z0-9')
suggestedBucketName="$s3BucketNamePrefix-$randomChars"
read -p "Please enter default stream destination bucket name [$suggestedBucketName]: " bucketName
bucketName=${bucketName:-$suggestedBucketName}

echo -n "Creating S3 Bucket: $bucketName.. "
aws s3 mb --region $region s3://$bucketName --output text | 
if [ $? -ne 0 ]; then
    exit 1
fi
echo "OK"

echo -n "Creating IAM Role: $iamRoleName.. "
roleArn=$(aws iam create-role --query "Role.Arn" --output text \
    --role-name $iamRoleName \
    --assume-role-policy-document "{
    \"Version\": \"2012-10-17\",
    \"Statement\": [
        {
            \"Sid\": \"PermitFirehoseAccess\",
            \"Effect\": \"Allow\",
            \"Principal\": {
                \"Service\": \"firehose.amazonaws.com\"
            },
            \"Action\": \"sts:AssumeRole\"
        }
    ]
}")
if [ $? -ne 0 ]; then
    aws s3api delete-bucket --region $region --bucket $bucketName
    exit 1
fi
echo "OK"

echo -n "Creating IAM Policy: $iamPolicyName.. "
aws iam put-role-policy \
    --role-name $iamRoleName \
    --policy-name $iamPolicyName \
    --policy-document "{
    \"Version\": \"2012-10-17\",  
    \"Statement\": [    
        {
            \"Sid\": \"PermitFirehoseUsage\",      
            \"Effect\": \"Allow\",      
            \"Action\": [        
                \"s3:AbortMultipartUpload\",        
                \"s3:GetBucketLocation\",        
                \"s3:GetObject\",        
                \"s3:ListBucket\",        
                \"s3:ListBucketMultipartUploads\",        
                \"s3:PutObject\"
            ],      
            \"Resource\": [        
                \"arn:aws:s3:::$bucketName\",
                \"arn:aws:s3:::$bucketName/*\"            
            ]    
        } 
    ]
}"
if [ $? -ne 0 ]; then
    aws iam delete-role --role-name $iamRoleName
    aws s3api delete-bucket --region $region --bucket $bucketName
    exit 1
fi
echo "OK"


echo "Waiting..."
sleep 30

echo -n "Creating Kinesis Firehose Delivery Stream: $deliveryStreamName with role arn $roleArn and bucket $bucketName.. "
deliveryStreamArn=$(aws firehose create-delivery-stream --region $region --query "DeliveryStreamARN" --output text \
    --delivery-stream-name $deliveryStreamName \
    --s3-destination-configuration "RoleARN=$roleArn,BucketARN=arn:aws:s3:::$bucketName")
if [ $? -ne 0 ]; then
    aws iam delete-role-policy \
        --role-name $iamRoleName \
        --policy-name $iamPolicyName
    aws iam delete-role --role-name $iamRoleName
    aws s3api delete-bucket --region $region --bucket $bucketName
    exit 1
fi
echo "OK"

echo "Delivery Stream ARN: $deliveryStreamArn"
