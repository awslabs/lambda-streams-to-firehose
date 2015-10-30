var debug = true;

var region = process.env['AWS_REGION'];

if (!region || region === null || region === "") {
	region = "us-east-1";
	console.log("Warning: Setting default region " + region);
}

if (debug) {
	console.log("AWS Kinesis Stream to Firehose Forwarder v" + pjson.version + " in " + region);
}

var aws = require('aws-sdk');
aws.config.update({
	region : region
});

var firehose = new aws.Firehose({
	apiVersion : '2015-08-04',
	region : 'eu-west-1'
});
var kinesis = new aws.Kinesis({
	apiVersion : '2013-12-02',
	region : region
});
var async = require('async');

var OK = 'OK';
var ERROR = 'ERROR';
var FORWARD_TO_FIREHOSE_STREAM = "ForwardToFirehoseStream";
var FIREHOSE_MAX_BATCH_SIZE = 500;

var deliveryStreamMapping = {};

/**
 * Example transformer that adds a newline to each event
 * 
 * Args:
 * 
 * data - base64 encoded Buffer containing kinesis data
 * 
 * callback(err,data) - callback to be called once transformation is completed.
 * data can be undefined if a filter is implemented
 */
exports.addNewlineTransformer = function(data, callback) {
	// emitting a new buffer as ascii text with newline
	callback(null, new Buffer(data.toString('ascii') + "\n"));
}

/**
 * Example transformer that converts a regular expression to delimited text
 */
exports.regexToDelimiter = function(regex, delimiter, data, callback) {
	var tokens = data.toString('ascii').match(regex);

	if (tokens) {
		callback(null, new Buffer(tokens.slice(1).join(delimiter) + "\n"));
	} else {
		callback(null, null);
	}
}
// var transformer = exports.addNewlineTransformer.bind(undefined);
//
// example regex transformer that matches all text after 'my regex' and turns it
// into pipe delimited text
var transformer = exports.regexToDelimiter.bind(undefined, "my regex (.*)", "|");

exports.handler = function(event, context) {
	/** Runtime Functions */
	var finish = function(event, status, message) {
		console.log("Processing Complete");

		// log the event if we've failed
		if (status !== OK) {
			if (message) {
				console.log(message);
			}

			// ensure that Lambda doesn't checkpoint to kinesis
			context.done(status, JSON.stringify(message));
		} else {
			context.done(null, message);
		}
	};

	/**
	 * function which forwards a batch of kinesis records to a firehose delivery
	 * stream after applying a user defined transformer
	 */
	exports.forwardKinesisBatch = function(processRecords, streamName, deliveryStreamName, afterBatchCallback) {
		// TODO remove this guard condition as it can only be encountered due to
		// a programming error
		if (processRecords.length > FIREHOSE_MAX_BATCH_SIZE) {
			afterBatchCallback("Cannot forward " + processRecords.length + " records to Firehose - max records for PutRecordBatch = " + FIREHOSE_MAX_BATCH_SIZE);
		}
		// END TODO

		// run the user defined transformer for each record to be processed
		async.map(processRecords, function(item, callback) {
			transformer(new Buffer(item.kinesis.data, 'base64'), function(err, transformed) {
				if (err) {
					console.log(JSON.stringify(err));
					callback(err, null);
				} else {
					if (transformed) {
						if (!(transformed instanceof Buffer)) {
							callback("Transformed must be an instance of Buffer", null);
						} else {
							callback(null, {
								Data : transformed
							});
						}
					}
				}
			});
		}, function(err, firehoseBatch) {
			if (err) {
				afterBatchCallback(err)
			} else {
				// write the batch to firehose with putRecordBatch
				var putRecordBatchParams = {
					DeliveryStreamName : deliveryStreamName,
					Records : firehoseBatch
				};
				firehose.putRecordBatch(putRecordBatchParams, function(err, data) {
					if (err) {
						console.log(JSON.stringify(err));
						afterBatchCallback(err);
					} else {
						if (debug) {
							console.log("Wrote " + firehoseBatch.length + " records to Firehose " + deliveryStreamName);
						}
						afterBatchCallback();
					}
				});
			}
		});
	};

	/**
	 * Convenience function which generates the batch set with low and high
	 * offsets for pushing data to Firehose in blocks of FIREHOSE_MAX_BATCH_SIZE
	 */
	exports.getBatchRanges = function(recordCount) {
		var batchCount = Math.floor(recordCount / FIREHOSE_MAX_BATCH_SIZE) + 1;
		var batches = [];

		for (var i = 0; i < batchCount; i++) {
			var batchLow = i * FIREHOSE_MAX_BATCH_SIZE;
			var batchHigh = batchLow + (FIREHOSE_MAX_BATCH_SIZE - 1);

			batches.push({
				lowOffset : batchLow,
				highOffset : batchHigh
			});
		}

		return batches;
	}

	/**
	 * Function to process a Kinesis Event from AWS Lambda, and generate
	 * requests to forward to Firehose
	 */
	exports.processEvent = function(event, streamName) {
		// look up the delivery stream name of the mapping cache
		var deliveryStreamName = deliveryStreamMapping[streamName];

		var batches = exports.getBatchRanges(event.Records.length);

		if (debug) {
			console.log("Forwarding " + event.Records.length + " Kinesis records to Delivery Stream " + deliveryStreamName);
			console.log(JSON.stringify(batchSpec));
		}

		// push to Firehose using PutRecords API at max record count. This uses
		// the async reduce method so that records from Kinesis will appear in
		// the Firehose PutRecords request in the same orderas they were
		// received by this function
		async.reduce(batches, 0, function(successCount, item, callback) {
			if (debug) {
				console.log("Forwarding records " + item.lowOffset + " to " + item.highOffset);
			}

			// grab the batch assigned subset of the records to push to firehose
			var processRecords = event.Records.slice(item.lowOffset, item.highOffset);

			exports.forwardKinesisBatch(processRecords, streamName, deliveryStreamName, function(err) {
				if (err) {
					callback(err, successCount);
				} else {
					callback(null, successCount++);
				}
			});
		}, function(err, result) {
			if (err) {
				console.log("Forwarding failure after " + result + " successful batches");
				finish(err, ERROR);
			} else {
				console.log("Event forwarding complete. Forwarded " + result + " batches to Firehose");
				finish(null, OK);
			}
		});
	};

	/**
	 * Function which resolves the destination delivery stream from the
	 * specified Kinesis Stream Name, using Tags
	 */
	exports.buildDeliveryMap = function(streamName, event, callback) {
		// get the delivery stream name from Kinesis tag
		kinesis.listTagsForStream({
			StreamName : streamName
		}, function(err, data) {
			if (err) {
				finish(event, ERROR, err);
			} else {
				data.Tags.map(function(item) {
					if (item.Key === FORWARD_TO_FIREHOSE_STREAM) {
						deliveryStreamMapping[streamName] = item.Value
					}
				});

				if (!deliveryStreamMapping[streamName]) {
					// fail as the stream isn't tagged for delivery, but as
					// there is an event source configured we think this should
					// have been done and is probably a misconfiguration
					finish(event, ERROR, "Warning: Kinesis Stream " + streamName + " not tagged for Firehose delivery with Tag name " + FORWARD_TO_FIREHOSE_STREAM);
				} else {
					// call the specified callback - should have already been
					// prepared by the calling function
					callback();
				}
			}
		});
	};
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
	}

	// only configured to support kinesis events. Maybe support SNS in future?
	if (event.Records[0].eventSource !== "aws:kinesis") {
		noProcessReason = "Invalid Event Source " + event.eventSource
	}

	// currently hard coded around the 1.0 kinesis event schema
	if (event.Records[0].kinesis.kinesisSchemaVersion !== "1.0") {
		noProcessReason = "Unsupported Event Schema Version " + event.Records[0].kinesis.kinesisSchemaVersion;
	}

	if (noProcessReason) {
		// terminate if there were any non process reasons
		finish(event, noProcessStatus, noProcessReason);
	} else {
		// parse the stream name out of the event
		var eventSourceARNTokens = event.Records[0].eventSourceARN.split(":");
		var streamName = eventSourceARNTokens[eventSourceARNTokens.length - 1].split("/")[1];

		if (deliveryStreamMapping.length == 0 || !deliveryStreamMapping[streamName]) {
			// no delivery stream cached so far, so add this stream's tag value
			// to the delivery map, and continue with processEvent
			exports.buildDeliveryMap(streamName, event, exports.processEvent.bind(undefined, event, streamName));
		} else {
			// delivery stream is cached
			exports.processEvent(event, streamName);
		}
	}
};