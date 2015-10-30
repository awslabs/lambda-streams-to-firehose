# Kinesis Streams to Kinesis Firehose Forwarder

Kinesis Streams gives customers the ability to process streaming big data at any scale with low latency and high data durability. Kinesis Firehose simplifies delivery of streaming data to Amazon S3 and Redshift with a simple, automatically scaled and zero operations requirement. Customers can also utilise the Kinesis Agent (citation) to automatically publish file data to Kinesis Streams and/or Kinesis Firehose delivery streams. For those customers who are already using Kinesis Streams for real time processing, and would also like to take advantage of Kinesis Firehose for archival of their Stream data, a simple way of pushing data from a Stream to a Firehose Delivery Stream is needed.

This project contains an AWS Lambda function which does just that, without any need for customer development. It is highly efficient and preserves Stream data ordering. The target Firehose Delivery Stream is referenced by tagging the Kinesis Stream with the Delivery Stream name to forward to.

# Pre-requisites

In order to effectively use this function, you should already have configured a Kinesis Stream, as well as a Kinesis Firehose Delivery Stream, and ensured that producer applications can write to the Stream, and that the Firehose Delivery Stream is able to deliver data to S3 or Redshift. This function makes no changes to Streams or Firehose configurations. You must also have the AWS Command Line Interface (citation) installed to take advantage of the Stream Tagging utility supplied

# Deploying

To use this function, simply deploy the [dist/KinesisStreamToFirehose-1.0.0.zip](KinesisStreamToFirehose-1.0.0.zip) to AWS Lambda. You must ensure that it is deployed with an invocation role that includes:

```
IAM Role
```

Finally, create an Event Source (citation) for this function from the Kinesis Stream to be forwarded to Firehose.

# Configuration

This Lambda function uses Tag information from Amazon Kinesis to determine which Delivery Stream to forward to. To Tag the Stream for Firehose Delivery, simply run the ```tagStream.sh``` script:

```
tagStream.sh <My Kinesis Stream> <My Firehose Delivery Stream> <region>

	<My Kinesis Stream> - The Kinesis Stream for which an event source has been created to the Forwarder Lambda function
	<My Firehose Delivery Stream> - The Delivery Stream which you've configured to deliver to the required destination
	<region> - The region in which the Kinesis Stream & Firehose Delivery Stream have been created. Today only single region operation is permitted
```

This will add a new Stream Tag named ```ForwardToFirehoseStream``` on the Kinesis Stream with the value you supply. You can run the script any time to update this value. To view the Tags configured on the Stream, simply run ```aws kinesis list-tags-for-stream --stream-name <My Kinesis Stream> --region <region>```

# Optional Data Transformation

By default, the Lambda function is configured to Base64 decode the Kinesis Stream data and append a newline character, so that files delivered to S3 are nicely formatted, and easy to load into Amazon Redshift. However, the function also provides a user extensible mechanism to write your own transformers. If you would like to modify the data after it's read from the Kinesis Stream, but before it's forwarded to Firehose, then you can implement and register a new Javascript function with the following interface:

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

# Confirming Successful Execution

When succesfully configured, writes to your Kinesis Stream should be automatically forwarded to the Firehose Delivery Stream, and you'll see data arriving in Amazon S3 and optionally Amazon Redshift. You can also view CloudWatch Logs (citation) for this Lambda function as it forwards streams

# Technical Bits

The Kinesis Streams to Firehose forwarding function uses the PutRecordBatch