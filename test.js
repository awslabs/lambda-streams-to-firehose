/*
		Copyright 2014-2015 Amazon.com, Inc. or its affiliates. All Rights Reserved.

    Licensed under the Amazon Software License (the "License"). You may not use this file except in compliance with the License. A copy of the License is located at

        http://aws.amazon.com/asl/

    or in the "license" file accompanying this file. This file is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, express or implied. See the License for the specific language governing permissions and limitations under the License. 
 */

var lambda = require('./index');

var measure = {
    "temp" : 16,
    "lng" : -2.046235208164378,
    "segmentId" : "IwjG",
    "corrosivityIndex" : 2.3403615330205645E-5,
    "sensorIp" : "10.0.10.98",
    "pressure" : 108.60358417008463,
    "incline" : 1.000000003881851E-8,
    "lat" : 57.6660648292971,
    "flow" : 13,
    "captureTs" : "2016-11-07 15:39:18",
    "sensorId" : "MUiv",
    "binaryValue" : "true"
};

var measureData = new Buffer(JSON.stringify(measure)).toString('base64');

event = {
    "Records" : [ {
	"eventID" : "shardId-000000000000:49545115243490985018280067714973144582180062593244200961",
	"eventVersion" : "1.0",
	"kinesis" : {
	    "approximateArrivalTimestamp" : 1428537600,
	    "partitionKey" : "partitionKey-3",
	    "data" : measureData,
	    "kinesisSchemaVersion" : "1.0",
	    "sequenceNumber" : "49545115243490985018280067714973144582180062593244200961"
	},
	"invokeIdentityArn" : "arn:aws:iam::EXAMPLE",
	"eventName" : "aws:kinesis:record",
	"eventSourceARN" : "arn:aws:kinesis:eu-west-1:887210671223:stream/TestRouting",
	"eventSource" : "aws:kinesis",
	"awsRegion" : "us-east-1"
    } ]
};

function context() {
}
context.done = function(status, message) {
    console.log("Context Closure Message: " + JSON.stringify(message));

    if (status && status !== null) {
	process.exit(-1);
    } else {
	process.exit(0);
    }
};

context.getRemainingTimeInMillis = function() {
    return 60000;
}

lambda.handler(event, context);