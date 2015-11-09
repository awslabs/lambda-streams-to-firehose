# AWS Streams to Kinesis Firehose Forwarder

Kinesis Firehose simplifies delivery of streaming data to Amazon S3 and Redshift with a simple, automatically scaled and zero operations service. Where customers have existing systems built on streaming interfaces, the addition of Firehose can enable simple archive, or be used to facilitate long term analysis of data from Amazon Redshift. Integration can be accomplished by using the Kinesis Agent (http://docs.aws.amazon.com/firehose/latest/dev/writing-with-agents.html) to automatically publish file data to Kinesis Streams and/or Kinesis Firehose delivery streams. This function enables customers who are already using Kinesis Streams for real time processing to take advantage of Kinesis Firehose. Furthermore, if you are using DynamoDB and would like to store a history of changes made to the table, this function can push events to Firehose.

![StreamToFirehose](StreamToFirehose.png)


# Pre-requisites

In order to effectively use this function, you should already have configured a Kinesis Stream or a DynamoDB Table with Update Streams, as well as a Kinesis Firehose Delivery Stream of the correct name. For Kinesis Streams, please ensure that producer applications can write to the Stream, and that the Firehose Delivery Stream is able to deliver data to S3 or Redshift. This function makes no changes to stream or Firehose configurations. You must also have the AWS Command Line Interface (https://aws.amazon.com/cli) installed to take advantage of the Stream Tagging utility supplied.

# Configuration

This Lambda function uses either Tag information from Amazon Kinesis Stream, or a convention to determine which Delivery Stream to forward to. If Kinesis Streams are used, the delivery stream can have any name, and to Tag the Stream for Firehose Delivery simply run the ```tagKinesisStream.sh``` script:

```
tagStream.sh <My Kinesis Stream> <My Firehose Delivery Stream> <region>

	<My Kinesis Stream> - The Kinesis Stream for which an event source has been created to the Forwarder Lambda function
	<My Firehose Delivery Stream> - The Delivery Stream which you've configured to deliver to the required destination
	<region> - The region in which the Kinesis Stream & Firehose Delivery Stream have been created. Today only single region operation is permitted
```

This will add a new Stream Tag named ```ForwardToFirehoseStream``` on the Kinesis Stream with the value you supply. You can run the script any time to update this value. To view the Tags configured on the Stream, simply run ```aws kinesis list-tags-for-stream --stream-name <My Kinesis Stream> --region <region>```

If you are using DynamoDB, then *the Firehose Delivery Stream must be the same name as the DynamoDB Table*.

Only single region deployments are supported today.

# Deploying

To use this function, simply deploy the [LambdaStreamToFirehose-1.1.0.zip](https://github.com/awslabs/kinesis-streams-to-firehose/blob/master/dist/LambdaStreamToFirehose-1.1.0.zip) to AWS Lambda. You must ensure that it is deployed with an invocation role that includes the ability to write CloudWatch Logs, Read from Kinesis or DynamoDB Streams, and Write to Kinesis Firehose:

```
{
  "Statement": [
    {
      "Action": [
        "logs:*"
      ],
      "Effect": "Allow",
      "Resource": [
        "*"
      ],
      "Sid": "Stmt1446202596000"
    },
    {
      "Action": [
        "kinesis:DescribeStream",
        "kinesis:ListStreams",
        "kinesis:GetShardIterator",
        "kinesis:GetRecords"
      ],
      "Effect": "Allow",
      "Resource": [
        "*"
      ],
      "Sid": "Stmt1446202612000"
    },
    {
      "Action": [
        "firehose:DescribeDeliveryStream",
        "firehose:ListDeliveryStreams",
        "firehose:PutRecord",
        "firehose:PutRecordBatch"
      ],
      "Effect": "Allow",
      "Resource": [
        "*"
      ],
      "Sid": "Stmt1446202630000"
    },
    {
      "Action": [
        "dynamodb:DescribeStream",
        "dynamodb:DescribeTable",
        "dynamodb:GetItem",
        "dynamodb:GetRecords",
        "dynamodb:GetShardIterator",
        "dynamodb:ListStreams",
        "dynamodb:ListTables"
      ],
      "Effect": "Allow",
      "Resource": [
        "*"
      ],
      "Sid": "Stmt1447079825000"
    }
  ],
  "Version": "myversion"
}
```

You may choose to restrict the IAM role to be specific to a subset of Kinesis or DynamoDB Update Streams and Firehose endpoints. 

Finally, create an Event Source (http://docs.aws.amazon.com/lambda/latest/dg/intro-core-components.html) for this function from the Stream to be forwarded to Firehose.

# Optional Data Transformation

This function can support streaming transformation your data. Kinesis data is Base64 decoded before being transformed, and when DynamoDB Update Streams are being forwarded, data will have the following structure:

```
{
Keys,
NewImage,
OldImage,
SequenceNumber,
SizeBytes,
eventName
}
```

For more information on DynamoDB Update Streams, please read http://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Streams.html.

By default, the function will append a newline character to received data so that files delivered to S3 are nicely formatted, and easy to load into Amazon Redshift. However, the function also provides a framework to write your own transformers. If you would like to modify the data after it's read from the Stream, but before it's forwarded to Firehose, then you can implement and register a new Javascript function with the following interface:

```
function(inputData, callback(err,outputData));

inputData: base64 encoded Buffer containing kinesis data
callback: function to be invoked once transformation is completed, with arguments:
	err: Any errors that were found during transformation
	outputData: Buffer instance (typically 'ascii' encoded) which will be forwarded to Firehose
```

You then register this transformer function by assigning an instance of it to the exported ```transformer``` instance:

```
var transformer = myTransformerFunction.bind(undefined, <internal setup args>);
```

You can also take advantage of a built in regex-to-csv transformer, which can be used by un-commenting and configuring the following entry in the function:
```
// var transformer = exports.regexToDelimiter.bind(undefined, /(myregex) (.*)/, "|");
```

Where ```/(myregex) (.*)/``` is the regular expression that uses character classes to capture data from the input stream to export to the CSV, and ```"|"``` is the delimiter.

# Confirming Successful Execution

When successfully configured, writes to your Stream will be automatically forwarded to the Firehose Delivery Stream, and you'll see data arriving in Amazon S3 and optionally Amazon Redshift. You can also view CloudWatch Logs (http://docs.aws.amazon.com/AmazonCloudWatch/latest/DeveloperGuide/WhatIsCloudWatchLogs.html) for this Lambda function as it forwards streams

# Debugging & Creating New Builds

If you write a new transformer, you may wish to see debug logging in the CloudWatch Logging Stream generated for function execution. If so, then simply change 'false' to 'true' on the first line of the function:

```
var debug = true;
```

You will then need to rebuild and redeploy the function. To do this, first install the required dependencies with ```npm install```, and then you can deploy a new version of the function with the ```build.sh``` script included in the repository. This will automatically redeploy the function using name 'KinesisStreamToFirehose'. If you have deployed your function as a different name, then please update this line in ```build.sh```

# Technical Bits

This function uses the putRecordBatch interface to Firehose to send 500 messages at a time with a max payload size of 4MB (as of 2015-11-02). The batches are processed serially so as to preserve the order of messages as they are received from the Stream.

Transformation creates another copy of input records, so you must plan accordingly when sizing the memory limit in AWS Lambda. Also consider that user defined transformers which significantly increase message size will need to be stored in memory before being dispatched to Firehose. If you need to limit the number of records for any reason, this can be set on the Event Source for your function.

----

AWS Streams to Firehose

Copyright 2015 Amazon.com, Inc. or its affiliates. All Rights Reserved.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.