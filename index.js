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

var aws = require('aws-sdk');
var firehose;
exports.firehose = firehose;
var kinesis;
exports.kinesis = kinesis;
var online = false;

var async = require('async');

var OK = 'OK';
var ERROR = 'ERROR';
var FORWARD_TO_FIREHOSE_STREAM = "ForwardToFirehoseStream";
var DDB_SERVICE_NAME = "aws:dynamodb";
var KINESIS_SERVICE_NAME = "aws:kinesis";
var FIREHOSE_MAX_BATCH_COUNT = 500;
// firehose max PutRecordBatch size 4MB
var FIREHOSE_MAX_BATCH_BYTES = 4 * 1024 * 1024;

var targetEncoding = "utf8";

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
 * Example transformer that adds a newline to each event
 * 
 * Args:
 * 
 * data - Object or string containing the data to be transformed
 * 
 * callback(err, Buffer) - callback to be called once transformation is
 * completed. If supplied callback is with a null/undefined output (such as
 * filtering) then nothing will be sent to Firehose
 */
exports.addNewlineTransformer = function(data, callback) {
	// emitting a new buffer as text with newline
	callback(null, new Buffer(data + "\n", targetEncoding));
};

/**
 * Example transformer that converts a regular expression to delimited text
 */
exports.regexToDelimiter = function(regex, delimiter, data, callback) {
	var tokens = JSON.stringify(data).match(regex);

	if (tokens) {
		// emitting a new buffer as delimited text whose contents are the regex
		// character classes
		callback(null, new Buffer(tokens.slice(1).join(delimiter) + "\n"));
	} else {
		callback("Configured Regular Expression does not match any tokens", null);
	}
};

/**
 * Example transformer that does nothing
 */
exports.identityTransformer = function(data, callback) {
  callback(null, new Buffer(data, targetEncoding));
};
//
// example regex transformer
// var transformer = exports.regexToDelimiter.bind(undefined, /(myregex) (.*)/,
// "|");

/*
 * create the transformer instance - change this to be regexToDelimter, or your
 * own new function
 */
var transformer = exports.identityTransformer.bind(undefined);

/**
 * Convenience function which generates the batch set with low and high offsets
 * for pushing data to Firehose in blocks of FIREHOSE_MAX_BATCH_COUNT and
 * staying within the FIREHOSE_MAX_BATCH_BYTES max payload size. Batch ranges
 * are calculated to be compatible with the array.slice() function which uses a
 * non-inclusive upper bound
 */
exports.getBatchRanges = function(records) {
	var batches = [];
	var currentLowOffset = 0;
	var batchCurrentBytes = 0;
	var batchCurrentCount = 0;
	var recordSize;

	for (var i = 0; i < records.length; i++) {
		// need to calculate the total record size for the call to Firehose on
		// the basis of of non-base64 encoded values
		recordSize = Buffer.byteLength(records[i].Data.toString(targetEncoding), targetEncoding);

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
};

/**
 * Function to create a condensed version of a dynamodb change record. This is
 * returned as a base64 encoded Buffer so as to implement the same interface
 * used for transforming kinesis records
 */
exports.createDynamoDataItem = function(record) {
	var output = {};
	output.Keys = record.dynamodb.Keys;

	if (record.dynamodb.NewImage)
		output.NewImage = record.dynamodb.NewImage;
	if (record.dynamodb.OldImage)
		output.OldImage = record.dynamodb.OldImage;

	// add the sequence number and other metadata
	output.SequenceNumber = record.dynamodb.SequenceNumber;
	output.SizeBytes = record.dynamodb.SizeBytes;
	output.eventName = record.eventName;

	return output;
};

/** function to extract the kinesis stream name from a kinesis stream ARN */
exports.getStreamName = function(arn) {
	try {
		var eventSourceARNTokens = arn.split(":");
		return eventSourceARNTokens[5].split("/")[1];
	} catch (e) {
		console.log("Malformed Kinesis Stream ARN");
		return;
	}
};

/** AWS Lambda event handler */
exports.handler = function(event, context) {
	var finish = function(event, status, message) {
		console.log("Processing Complete");

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
	};

	/**
	 * function which handles the output of the defined transformation on each
	 * record.
	 */
	exports.processTransformedRecords = function(transformed, streamName, deliveryStreamName) {
		if (debug) {
			console.log('Processing transformed records');
		}
		// get the set of batch offsets based on the transformed record sizes
		var batches = exports.getBatchRanges(transformed);

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
			var processRecords = transformed.slice(item.lowOffset, item.highOffset);

			exports.writeToFirehose(processRecords, streamName, deliveryStreamName, function(err) {
				if (err) {
					reduceCallback(err, successCount);
				} else {
					reduceCallback(null, successCount + 1);
				}
			});
		}, function(err, result) {
			if (err) {
				console.log("Forwarding failure after " + result + " successful batches");
				finish(err, ERROR);
			} else {
				console.log("Event forwarding complete. Forwarded " + result + " batches comprising " + transformed.length + " records to Firehose " + deliveryStreamName);
				finish(null, OK);
			}
		});
	};

	/**
	 * function which forwards a batch of kinesis records to a firehose delivery
	 * stream
	 */
	exports.writeToFirehose = function(firehoseBatch, streamName, deliveryStreamName, callback) {
		if (debug) {
			console.log('Writing to firehose');
		}
		// write the batch to firehose with putRecordBatch
		var putRecordBatchParams = {
			DeliveryStreamName : deliveryStreamName,
			Records : firehoseBatch
		};

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
	};

	/**
	 * Function to process a stream event received by AWS Lambda, and generate
	 * requests to forward to Firehose
	 */
	exports.processEvent = function(event, serviceName, streamName) {
		if (debug) {
			console.log('Processing event');
		}
		// look up the delivery stream name of the mapping cache
		var deliveryStreamName = deliveryStreamMapping[streamName];

		if (debug) {
			console.log("Forwarding " + event.Records.length + " " + serviceName + " records to Delivery Stream " + deliveryStreamName);
		}

		async.map(event.Records, function(item, recordCallback) {
			// resolve the record data based on the service
			if (serviceName === KINESIS_SERVICE_NAME) {
				// run the record through the KPL deaggregator
				deagg.deaggregateSync(item.kinesis, computeChecksums, function(err, userRecords) {
					// userRecords now has all the deaggregated user records, or
					// just the original record if no KPL aggregation is in
					// use
					if (err) {
						recordCallback(err);
					} else {
						recordCallback(null, userRecords);
					}
				});
			} else {
				// dynamo update stream record
				var data = exports.createDynamoDataItem(item);

				recordCallback(null, data);
			}
		}, function(err, extractedUserRecords) {
			if (err) {
				finish(err, ERROR);
			} else {
				// extractedUserRecords will be array[array[Object]], so
				// flatten to array[Object]
				var userRecords = [].concat.apply([], extractedUserRecords);

				// transform the user records
				async.map(userRecords, function(userRecord, userRecordCallback) {
					var dataItem = serviceName === KINESIS_SERVICE_NAME ? new Buffer(userRecord.data, 'base64').toString(targetEncoding) : userRecord;

					// only transform the data portion of a kinesis record, or
					// the entire dynamo record
					transformer(dataItem, function(err, transformed) {
						if (err) {
							console.log(JSON.stringify(err));
							userRecordCallback(err);
						} else {
							if (transformed) {
								// transformed should be a buffer type, as we
								// want to limit the need for preprocessing
								// before emitting to firehose
								if (!(transformed instanceof Buffer)) {
									userRecordCallback("Output of Transformer must be an instance of Buffer");
								} else {
									// call the map callback with the
									// transformed Buffer
									// decorated for use as a Firehose batch
									// entry
									userRecordCallback(null, {
										Data : transformed
									});
								}
							}
						}
					});
				}, function(err, transformed) {
					// user records have now been transformed, so call
					// errors or invoke the transformed record processor
					if (err) {
						finish(err, ERROR);
					} else {
						exports.processTransformedRecords(transformed, streamName, deliveryStreamName);
					}
				});
			}
		});
	};

	/**
	 * Function which resolves the destination delivery stream from the
	 * specified Kinesis Stream Name, using Tags
	 */
	exports.buildDeliveryMap = function(streamName, serviceName, event, callback) {
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
							 * Disable fallback to a default delivery stream as
							 * a FORWARD_TO_FIREHOSE_STREAM has been
							 * specifically set.
							 */
							shouldFailbackToDefaultDeliveryStream = false;
							deliveryStreamMapping[streamName] = item.Value;
						}
					});

					exports.verifyDeliveryStreamMapping(streamName, shouldFailbackToDefaultDeliveryStream, event, callback);
				}
			});
		}
	};

	exports.verifyDeliveryStreamMapping = function(streamName, shouldFailbackToDefaultDeliveryStream, event, callback) {
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
				 * Fail as no delivery stream mapping has been specified and we
				 * have not configured to use a default. Kinesis Streams should
				 * be tagged with ForwardToFirehoseStream = <DeliveryStreamName>
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
		finish(event, noProcessStatus, noProcessReason);
	} else {
		init();

		// parse the stream name out of the event
		var streamName = exports.getStreamName(event.Records[0].eventSourceARN);
		if (deliveryStreamMapping.length === 0 || !deliveryStreamMapping[streamName]) {
			// no delivery stream cached so far, so add this stream's tag value
			// to the delivery map, and continue with processEvent
			exports.buildDeliveryMap(streamName, serviceName, event, exports.processEvent.bind(undefined, event, serviceName, streamName));
		} else {
			// delivery stream is cached
			exports.processEvent(event, serviceName, streamName);
		}
	}
};
