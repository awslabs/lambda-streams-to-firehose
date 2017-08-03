require('./constants');

var debug = process.env.DEBUG || false;

/** function which will simply route records to the provided delivery stream */
function defaultRouting(defaultDeliveryStreamName, records, callback) {
    var routingMap = {};
    routingMap[defaultDeliveryStreamName] = records;
    callback(null, routingMap);
}
exports.defaultRouting = defaultRouting;

/**
 * Function to apply a routing function to a group of records
 *
 * @param defaultDeliveryStreamName
 * @param records
 * @param routingFunction
 * @param callback
 * @returns
 */
function routeToDestination(defaultDeliveryStreamName, records, routingFunction, callback) {
    routingFunction.call(undefined, defaultDeliveryStreamName, records, callback);
}
exports.routeToDestination = routeToDestination;

/**
 * Helper function to add scalar values to an object which contains indexed
 * arrays
 *
 * @param key
 * @param value
 * @param intoObject
 * @returns
 */
function pushArrayValue(key, value, intoObject) {
    if (!key in intoObject || !intoObject[key]) {
	intoObject[key] = []
    }
    intoObject[key].push(value);
}

/**
 * Function which routes to a destination by attribute mapping. Only routing
 * which uses a synchronous call is currently supported
 *
 * @param attributeMap
 * @param defaultDeliveryStreamName
 * @param records
 * @param callback
 * @returns
 */
function routeByAttributeMapping(attributeMap, defaultDeliveryStreamName, records, callback) {
    // validate that this is a sensible attribute map - currently supports just
    // one attribute
    //
    // example attributeMap
    // "myRoutableAttribute": {
    // "value1":"deliveryStream1",
    // "value2":"deliveryStream2"
    // }
    if (Object.keys(attributeMap).length != 1) {
	callback("Invalid Attribute Map. Must be at most one attribute name");
    } else {
	// route to the destination first matching in the list
	var key = Object.keys(attributeMap)[0];

	if (debug) {
	    console.log("Routing on the basis of Attribute: " + key);
	}
	var routingMap = {};

	// route each record
	records.map(function(record) {
	    try {
		var jsonRecord = JSON.parse(new Buffer(record, 'base64').toString('utf8'));

		if (!key in jsonRecord) {
		    // this object doesn't include the routed attribute, so
		    // go to default
		    pushArrayValue(defaultDeliveryStreamName, record, routingMap);

		    if (debug) {
			console.log("Routing to default Delivery Stream as payload does not include '" + key + "'");
		    }
		} else {
		    var recordValue = jsonRecord[key];
		    var targetDeliveryStream;

		    // read the json object's value out of the attribute map, if
		    // it's there, and use it as the destination. If not then
		    // assign the default delivery stream
		    if (recordValue in attributeMap[key]) {
			targetDeliveryStream = attributeMap[key][recordValue];
		    } else {
			targetDeliveryStream = defaultDeliveryStreamName;
		    }

		    if (debug) {
			console.log("Resolved destination Delivery Stream " + targetDeliveryStream + " by value " + attributeMap[key][recordValue]);
		    }
		    pushArrayValue(targetDeliveryStream, record, routingMap);
		}
	    } catch (err) {
		console.log(err); // unknown error - just push the original
		// buffer to the default // delivery stream
		pushArrayValue(defaultDeliveryStreamName, record, routingMap);
	    }
	});

	if (debug) {
	    console.log(routingMap);
	}

	callback(null, routingMap);
    }
}
exports.routeByAttributeMapping = routeByAttributeMapping;
