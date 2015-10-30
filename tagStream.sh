#!/bin/bash

if [ $# != 3 ]; then
	echo "Please provide the Stream Name, the Delivery Stream to forward to, and the Region"
	exit -1
fi

which aws > /dev/null 2>&1

if [ $? != 0 ]; then
	echo "This utility requires the AWS Cli, which can be installed using instructions found at http://docs.aws.amazon.com/cli/latest/userguide/installing.html"
	exit -2
fi

aws kinesis add-tags-to-stream --stream-name $1 --Tags ForwardToFirehoseStream=$2 --region $3

aws kinesis list-tags-for-stream --stream-name $1 --region $3