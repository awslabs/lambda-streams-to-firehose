#!/bin/bash
#set -x

if [ "$#" == 0 -o "$1" == "" ]; then
	echo "You must provide the version number to upload, and the AWS CLI profile name to use"
	exit -1
fi

regions=`aws ec2 describe-regions --query Regions[*].RegionName --output text`

aws s3 cp dist/LambdaStreamToFirehose-$1.zip s3://aws-lambda-streams-to-firehose/LambdaStreamToFirehose-$1.zip --acl public-read --profile $2

echo "Created base distribution in us-east-1"

for region in `echo $regions`; do
	echo "Copying distribution to $region"
	aws s3 cp s3://aws-lambda-streams-to-firehose/LambdaStreamToFirehose-$1.zip s3://awslabs-code-$region/LambdaStreamToFirehose/LambdaStreamToFirehose-$1.zip --source-region us-east-1 --region $region --profile $2 --acl public-read
done
