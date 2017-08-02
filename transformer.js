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
var async = require('async');
require('./constants');

var debug = process.env.DEBUG || false;

function addNewlineTransformer(data, callback) {
    // emitting a new buffer as text with newline
    callback(null, new Buffer(data + "\n", targetEncoding));
};
exports.addNewlineTransformer = addNewlineTransformer;

/** Convert JSON data to its String representation */
function jsonToStringTransformer(data, callback) {
    // emitting a new buffer as text with newline
    callback(null, new Buffer(JSON.stringify(data) + "\n", targetEncoding));
};
exports.jsonToStringTransformer = jsonToStringTransformer;

/** literally nothing at all transformer - just wrap the object in a buffer */
function doNothingTransformer(data, callback) {
    // emitting a new buffer as text with newline
    callback(null, new Buffer(data, targetEncoding));
};
exports.doNothingTransformer = doNothingTransformer;

/**
 * Example transformer that converts a regular expression to delimited text
 */
function regexToDelimiter(regex, delimiter, data, callback) {
    var tokens = JSON.stringify(data).match(regex);

    if (tokens) {
	// emitting a new buffer as delimited text whose contents are the regex
	// character classes
	callback(null, new Buffer(tokens.slice(1).join(delimiter) + "\n"));
    } else {
	callback("Configured Regular Expression does not match any tokens", null);
    }
};
exports.regexToDelimiter = regexToDelimiter;

//
// example regex transformer
// var transformer = exports.regexToDelimiter.bind(undefined, /(myregex) (.*)/,
// "|");

function transformRecords(serviceName, transformer, userRecords, callback) {
    async.map(userRecords, function(userRecord, userRecordCallback) {
	var dataItem = serviceName === KINESIS_SERVICE_NAME ? new Buffer(userRecord.data, 'base64').toString(targetEncoding) : userRecord;

	transformer.call(undefined, dataItem, function(err, transformed) {
	    if (err) {
		console.log(JSON.stringify(err));
		userRecordCallback(err);
	    } else {
		if (transformed && transformed instanceof Buffer) {
		    // call the map callback with the
		    // transformed Buffer decorated for use as a
		    // Firehose batch entry
		    userRecordCallback(null, transformed);
		} else {
		    // don't know what this transformed
		    // object is
		    userRecordCallback("Output of Transformer was malformed. Must be instance of Buffer or routable Object");
		}
	    }
	});
    }, function(err, transformed) {
	// user records have now been transformed, so call
	// errors or invoke the transformed record processor
	if (err) {
	    callback(err);
	} else {
	    callback(null, transformed);
	}
    });
};
exports.transformRecords = transformRecords;

function setupTransformer(callback) {
    // set the default transformer
    var t = jsonToStringTransformer.bind(undefined);

    // set the transformer based on specified datatype of the stream
    if (process.env[STREAM_DATATYPE_ENV]) {
	var found = false;
	Object.keys(supportedDatatypeTransformerMappings).forEach(function(key) {
	    if (process.env[STREAM_DATATYPE_ENV] === key) {
		found = true;
	    }
	});

	if (found === true) {
	    // set the transformer class via a cross reference to the
	    // transformer mapping
	    if (debug) {
		console.log("Setting data transformer based on Stream Datatype configuration");
	    }
	    t = this[transformerRegistry[supportedDatatypeTransformerMappings[process.env[STREAM_DATATYPE_ENV]]]].bind(undefined);
	}
    } else {
	if (debug) {
	    console.log("No Stream Datatype Environment Configuration found");
	}
    }

    // check if the transformer has been overridden by environment settings
    if (process.env[TRANSFORMER_FUNCTION_ENV]) {
	console.log(process.env[TRANSFORMER_FUNCTION_ENV])
	var found = false;
	Object.keys(transformerRegistry).forEach(function(key) {
	    if (process.env[TRANSFORMER_FUNCTION_ENV] === transformerRegistry[key]) {
		found = true;
	    }
	});

	if (found === false) {
	    callback("Configured Transformer function " + process.env[TRANSFORMER_FUNCTION_ENV] + " is not a valid transformation method in the transformer.js module");
	} else {
	    if (debug) {
		console.log("Setting data transformer based on Transformer Override configuration");
	    }
	    // dynamically bind in the transformer function
	    t = this[process.env[TRANSFORMER_FUNCTION_ENV]].bind(undefined);
	}
    } else {
	if (debug) {
	    console.log("No Transformer Override Environment Configuration found");
	}
    }

    if (debug) {
	console.log("Using Transformer function " + t.name);
    }

    callback(null, t);
}
exports.setupTransformer = setupTransformer;
