/*
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
 */
var debug = false;

var pjson = require('./package.json');
var setRegion = process.env['AWS_REGION'];
var deagg = require('aws-kpl-deagg');
var async = require('async');

var aws = require('aws-sdk');
var firehose;
exports.firehose = firehose;
var kinesis;
exports.kinesis = kinesis;
var online = false;

require('./constants');

/* Configure transform utility */
var transform = require('./transformer');
/*
 * create the transformer instance - change this to be regexToDelimter, or your
 * own new function
 */
var useTransformer = transform.jsonToStringTransformer.bind(undefined);

/*
 * Configure destination router. By default all records route to the configured
 * stream
 */
var router = require('./router');

/*
 * create the routing rule reference that you want to use. This shows the
 * default router
 */
var useRouter = router.defaultRouting.bind(undefined);
// example for using routing based on messages attributes
// var attributeMap = {
// "binaryValue" : {
// "true" : "TestRouting-route-A",
// "false" : "TestRouting-route-B"
// }
// };
// var useRouter = router.routeByAttributeMapping.bind(undefined, attributeMap);

// should KPL checksums be calculated?
var computeChecksums = true;

/*
 * If the source Kinesis Stream's tags or DynamoDB Stream Name don't resolve to
 * an existing Firehose, allow usage of a default delivery stream, or fail with
 * an error.
 */
var USE_DEFAULT_DELIVERY_STREAMS = true;
/*
 * Delivery stream mappings can be specified here to overwrite values provided
 * by Kinesis Stream tags or DynamoDB stream name. (Helpful for debugging)
 * Format: DDBStreamName: deliveryStreamName Or: FORWARD_TO_FIREHOSE_STREAM tag
 * value: deliveryStreamName
 */
var deliveryStreamMapping = {
    'DEFAULT' : 'LambdaStreamsDefaultDeliveryStream'
};

var start;

function init() {
    if (!online) {
	if (!setRegion || setRegion === null || setRegion === "") {
	    setRegion = "us-east-1";
	    console.log("Warning: Setting default region " + setRegion);
	}

	if (debug) {
	    console.log("AWS Streams to Firehose Forwarder v" + pjson.version + " in " + setRegion);
	}

	aws.config.update({
	    region : setRegion
	});

	// configure a new connection to firehose, if one has not been provided
	if (!exports.firehose) {
	    if (debug) {
		console.log("Connecting to Amazon Kinesis Firehose in " + setRegion);
	    }
	    exports.firehose = new aws.Firehose({
		apiVersion : '2015-08-04',
		region : setRegion
	    });
	}

	// configure a new connection to kinesis streams, if one has not been
	// provided
	if (!exports.kinesis) {
	    if (debug) {
		console.log("Connecting to Amazon Kinesis Streams in " + setRegion);
	    }
	    exports.kinesis = new aws.Kinesis({
		apiVersion : '2013-12-02',
		region : setRegion
	    });
	}

	online = true;
    }
}

/**
 * Function to create a condensed version of a dynamodb change record. This is
 * returned as a base64 encoded Buffer so as to implement the same interface
 * used for transforming kinesis records
 */
function createDynamoDataItem(record) {
    var output = {};
    output.Keys = record.dynamodb.Keys;

    if (record.dynamodb.NewImage)
	output.NewImage = record.dynamodb.NewImage;
    if (record.dynamodb.OldImage)
	output.OldImage = record.dynamodb.OldImage;

    // add the sequence number and other metadata
    output.SequenceNumber = record.dynamodb.SequenceNumber;
    output.SizeBytes = record.dynamodb.SizeBytes;
    output.ApproximateCreationDateTime = record.dynamodb.ApproximateCreationDateTime;
    output.eventName = record.eventName;

    return output;
}
exports.createDynamoDataItem = createDynamoDataItem;

/** function to extract the kinesis stream name from a kinesis stream ARN */
function getStreamName(arn) {
    try {
	var eventSourceARNTokens = arn.split(":");
	return eventSourceARNTokens[5].split("/")[1];
    } catch (e) {
	console.log("Malformed Kinesis Stream ARN");
	return;
    }
}
exports.getStreamName = getStreamName;

function onCompletion(context, event, err, status, message) {
    console.log("Processing Complete");

    if (err) {
	console.log(err);
    }

    // log the event if we've failed
    if (status !== OK) {
	if (message) {
	    console.log(message);
	}

	// ensure that Lambda doesn't checkpoint to kinesis on error
	context.done(status, JSON.stringify(message));
    } else {
	context.done(null, message);
    }
}

/** AWS Lambda event handler */
function handler(event, context) {
    // add the context and event to the function that closes the lambda
    // invocation
    var finish = onCompletion.bind(undefined, context, event);

    /** End Runtime Functions */
    if (debug) {
	console.log(JSON.stringify(event));
    }

    // fail the function if the wrong event source type is being sent, or if
    // there is no data, etc
    var noProcessStatus = ERROR;
    var noProcessReason;

    if (!event.Records || event.Records.length === 0) {
	noProcessReason = "Event contains no Data";
	// not fatal - just got an empty event
	noProcessStatus = OK;
    } else {
	// there are records in this event
	var serviceName;
	if (event.Records[0].eventSource === KINESIS_SERVICE_NAME || event.Records[0].eventSource === DDB_SERVICE_NAME) {
	    serviceName = event.Records[0].eventSource;
	} else {
	    noProcessReason = "Invalid Event Source " + event.Records[0].eventSource;
	}

	// currently hard coded around the 1.0 kinesis event schema
	if (event.Records[0].kinesis && event.Records[0].kinesis.kinesisSchemaVersion !== "1.0") {
	    noProcessReason = "Unsupported Kinesis Event Schema Version " + event.Records[0].kinesis.kinesisSchemaVersion;
	}
    }

    if (noProcessReason) {
	// terminate if there were any non process reasons
	finish(noProcessStatus, ERROR, noProcessReason);
    } else {
	init();

	// parse the stream name out of the event
	var streamName = exports.getStreamName(event.Records[0].eventSourceARN);

	// create the processor to handle each record
	var processor = exports.processEvent.bind(undefined, event, serviceName, streamName, function(err) {
	    if (err) {
		finish(err, ERROR, "Error Processing Records");
	    } else {
		finish(undefined, OK);
	    }
	});

	if (deliveryStreamMapping.length === 0 || !deliveryStreamMapping[streamName]) {
	    // no delivery stream cached so far, so add this stream's tag value
	    // to the delivery map, and continue with processEvent
	    exports.buildDeliveryMap(streamName, serviceName, event, processor);
	} else {
	    // delivery stream is cached so just invoke the processor
	    processor();
	}
    }
}
exports.handler = handler;

/**
 * Function which resolves the destination delivery stream for a given Kinesis
 * stream. If no delivery stream is found to deliver to, then we will cache the
 * default delivery stream
 * 
 * @param streamName
 * @param shouldFailbackToDefaultDeliveryStream
 * @param event
 * @param callback
 * @returns
 */
function verifyDeliveryStreamMapping(streamName, shouldFailbackToDefaultDeliveryStream, event, callback) {
    if (debug) {
	console.log('Verifying delivery stream');
    }
    if (!deliveryStreamMapping[streamName]) {
	if (shouldFailbackToDefaultDeliveryStream) {
	    /*
	     * No delivery stream has been specified, probably as it's not
	     * configured in stream tags. Using default delivery stream. To
	     * prevent accidental forwarding of streams to a firehose set
	     * USE_DEFAULT_DELIVERY_STREAMS = false.
	     */
	    deliveryStreamMapping[streamName] = deliveryStreamMapping['DEFAULT'];
	} else {
	    /*
	     * Fail as no delivery stream mapping has been specified and we have
	     * not configured to use a default. Kinesis Streams should be tagged
	     * with ForwardToFirehoseStream = <DeliveryStreamName>
	     */
	    finish(event, ERROR, "Warning: Kinesis Stream " + streamName + " not tagged for Firehose delivery with Tag name " + FORWARD_TO_FIREHOSE_STREAM);
	    return;
	}
    }
    // validate the delivery stream name provided
    var params = {
	DeliveryStreamName : deliveryStreamMapping[streamName]
    };
    exports.firehose.describeDeliveryStream(params, function(err, data) {
	if (err) {
	    if (shouldFailbackToDefaultDeliveryStream) {
		deliveryStreamMapping[streamName] = deliveryStreamMapping['DEFAULT'];
		exports.verifyDeliveryStreamMapping(streamName, false, event, callback);
	    } else {
		finish(event, ERROR, "Could not find suitable delivery stream for " + streamName + " and the " + "default delivery stream (" + deliveryStreamMapping['DEFAULT']
			+ ") either doesn't exist or is disabled.");
	    }
	} else {
	    // call the specified callback - should have
	    // already
	    // been prepared by the calling function
	    callback();
	}
    });
}
exports.verifyDeliveryStreamMapping = verifyDeliveryStreamMapping;

/**
 * Function which resolves the destination delivery stream from the specified
 * Kinesis Stream Name, using Tags attached to the Kinesis Stream
 */
function buildDeliveryMap(streamName, serviceName, event, callback) {
    if (debug) {
	console.log('Building delivery stream mapping');
    }
    if (deliveryStreamMapping[streamName]) {
	// A delivery stream has already been specified in configuration
	// This could be indicative of debug usage.
	exports.verifyDeliveryStreamMapping(streamName, false, event, callback);
    } else if (serviceName === DDB_SERVICE_NAME) {
	// dynamodb streams need the firehose delivery stream to match
	// the table name
	deliveryStreamMapping[streamName] = streamName;
	exports.verifyDeliveryStreamMapping(streamName, USE_DEFAULT_DELIVERY_STREAMS, event, callback);
    } else {
	// get the delivery stream name from Kinesis tag
	exports.kinesis.listTagsForStream({
	    StreamName : streamName
	}, function(err, data) {
	    shouldFailbackToDefaultDeliveryStream = USE_DEFAULT_DELIVERY_STREAMS;
	    if (err) {
		finish(event, ERROR, err);
	    } else {
		// grab the tag value if it's the foreward_to_firehose
		// name item
		data.Tags.map(function(item) {
		    if (item.Key === FORWARD_TO_FIREHOSE_STREAM) {
			/*
			 * Disable fallback to a default delivery stream as a
			 * FORWARD_TO_FIREHOSE_STREAM has been specifically set.
			 */
			shouldFailbackToDefaultDeliveryStream = false;
			deliveryStreamMapping[streamName] = item.Value;
		    }
		});

		exports.verifyDeliveryStreamMapping(streamName, shouldFailbackToDefaultDeliveryStream, event, callback);
	    }
	});
    }
}
exports.buildDeliveryMap = buildDeliveryMap;

/**
 * Convenience function which generates the batch set with low and high offsets
 * for pushing data to Firehose in blocks of FIREHOSE_MAX_BATCH_COUNT and
 * staying within the FIREHOSE_MAX_BATCH_BYTES max payload size. Batch ranges
 * are calculated to be compatible with the array.slice() function which uses a
 * non-inclusive upper bound
 */
function getBatchRanges(records) {
    var batches = [];
    var currentLowOffset = 0;
    var batchCurrentBytes = 0;
    var batchCurrentCount = 0;
    var recordSize;

    for (var i = 0; i < records.length; i++) {
	// need to calculate the total record size for the call to Firehose on
	// the basis of of non-base64 encoded values
	recordSize = Buffer.byteLength(records[i].toString(targetEncoding), targetEncoding);

	// batch always has 1 entry, so add it first
	batchCurrentBytes += recordSize;
	batchCurrentCount += 1;

	// generate a new batch marker every 4MB or 500 records, whichever comes
	// first
	if (batchCurrentCount === FIREHOSE_MAX_BATCH_COUNT || batchCurrentBytes + recordSize > FIREHOSE_MAX_BATCH_BYTES || i === records.length - 1) {
	    batches.push({
		lowOffset : currentLowOffset,
		// annoying special case handling for record sets of size 1
		highOffset : i + 1,
		sizeBytes : batchCurrentBytes
	    });
	    // reset accumulators
	    currentLowOffset = i + 1;
	    batchCurrentBytes = 0;
	    batchCurrentCount = 0;
	}
    }

    return batches;
}
exports.getBatchRanges = getBatchRanges;

/**
 * Function to process a stream event and generate requests to forward the
 * embedded records to Kinesis Firehose. Before delivery, the user specified
 * transformer will be invoked, and the messages will be passed through a router
 * which can determine the delivery stream dynamically if needed
 */
function processEvent(event, serviceName, streamName, callback) {
    if (debug) {
	console.log('Processing event');
    }
    // look up the delivery stream name of the mapping cache
    var deliveryStreamName = deliveryStreamMapping[streamName];

    if (debug) {
	console.log("Forwarding " + event.Records.length + " " + serviceName + " records to Delivery Stream " + deliveryStreamName);
    }

    async.map(event.Records, function(record, recordCallback) {
	// resolve the record data based on the service
	if (serviceName === KINESIS_SERVICE_NAME) {
	    // run the record through the KPL deaggregator
	    deagg.deaggregateSync(record.kinesis, computeChecksums, function(err, userRecords) {
		// userRecords now has all the deaggregated user records, or
		// just the original record if no KPL aggregation is in use
		if (err) {
		    recordCallback(err);
		} else {
		    recordCallback(null, userRecords);
		}
	    });
	} else {
	    // dynamo update stream record
	    var data = exports.createDynamoDataItem(record);

	    recordCallback(null, data);
	}
    }, function(err, extractedUserRecords) {
	if (err) {
	    callback(err);
	} else {
	    // extractedUserRecords will be array[array[Object]], so
	    // flatten to array[Object]
	    var userRecords = [].concat.apply([], extractedUserRecords);

	    // transform the user records
	    transform.transformRecords(serviceName, useTransformer, userRecords, function(err, transformed) {
		// apply the routing function that has been configured
		router.routeToDestination(deliveryStreamName, transformed, useRouter, function(err, routingDestinationMap) {
		    if (err) {
			// we are still going to route to the default stream
			// here, as a bug in routing implementation cannot
			// result in lost data!
			console.log(err);

			// discard the delivery map we might have received
			routingDestinationMap[deliveryStreamName] = transformed;
		    }
		    // send the routed records to the delivery processor
		    async.map(Object.keys(routingDestinationMap), function(destinationStream, asyncCallback) {
			var records = routingDestinationMap[destinationStream];

			processFinalRecords(records, streamName, destinationStream, asyncCallback);
		    }, function(err, results) {
			if (err) {
				callback(err);
			} else {
				if (debug) {
					results.map(function(item) {
						console.log(JSON.stringify(item));
					});
				}
				callback();
			}
		    });

		});
	    });
	}
    });
}
exports.processEvent = processEvent;

/**
 * function which forwards a batch of kinesis records to a firehose delivery
 * stream
 */
function writeToFirehose(firehoseBatch, streamName, deliveryStreamName, callback) {
    // write the batch to firehose with putRecordBatch
    var putRecordBatchParams = {
	DeliveryStreamName : deliveryStreamName.substring(0, 64),
	Records : firehoseBatch
    };

    if (debug) {
	console.log('Writing to firehose delivery stream ');
	console.log(JSON.stringify(putRecordBatchParams));
    }

    var startTime = new Date().getTime();
    exports.firehose.putRecordBatch(putRecordBatchParams, function(err, data) {
	if (err) {
	    console.log(JSON.stringify(err));
	    callback(err);
	} else {
	    if (debug) {
		var elapsedMs = new Date().getTime() - startTime;
		console.log("Successfully wrote " + firehoseBatch.length + " records to Firehose " + deliveryStreamName + " in " + elapsedMs + " ms");
	    }
	    callback();
	}
    });
}
exports.writeToFirehose = writeToFirehose;

/**
 * function which handles the output of the defined transformation on each
 * record.
 */
function processFinalRecords(records, streamName, deliveryStreamName, callback) {
    if (debug) {
	console.log('Delivering records to destination Streams');
    }
    // get the set of batch offsets based on the transformed record sizes
    var batches = exports.getBatchRanges(records);

    if (debug) {
	console.log(JSON.stringify(batches));
    }

    // push to Firehose using PutRecords API at max record count or size.
    // This uses the async reduce method so that records from Kinesis will
    // appear in the Firehose PutRecords request in the same order as they
    // were received by this function
    async.reduce(batches, 0, function(successCount, item, reduceCallback) {
	if (debug) {
	    console.log("Forwarding records " + item.lowOffset + ":" + item.highOffset + " - " + item.sizeBytes + " Bytes");
	}

	// grab subset of the records assigned for this batch and push to
	// firehose
	var processRecords = records.slice(item.lowOffset, item.highOffset);

	// decorate the array for the Firehose API
	var decorated = [];
	processRecords.map(function(item) {
	    decorated.push({
		Data : item
	    });
	});

	exports.writeToFirehose(decorated, streamName, deliveryStreamName, function(err) {
	    if (err) {
		reduceCallback(err, successCount);
	    } else {
		reduceCallback(null, successCount + 1);
	    }
	});
    }, function(err, successfulBatches) {
	if (err) {
	    console.log("Forwarding failure after " + successfulBatches + " successful batches");
	    callback(err);
	} else {
	    console.log("Event forwarding complete. Forwarded " + successfulBatches + " batches comprising " + records.length + " records to Firehose " + deliveryStreamName);
	    callback(null, {
		"deliveryStreamName" : deliveryStreamName,
		"batchesDelivered" : successfulBatches,
		"recordCount" : records.length
	    });
	}
    });
}
exports.processFinalRecords = processFinalRecords;