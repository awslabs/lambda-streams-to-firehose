var assert = require('assert');
var router = require('../router.js')

describe('Routing tests', function() {
    var defaultDeliveryStream = "MyDeliveryStream";

    describe('Default Routing', function() {

	var records = [ new Buffer("IanTest1").toString('base64'), new Buffer("IanTest2").toString('base64') ];

	router.routeToDestination(defaultDeliveryStream, records, router.defaultRouting.bind(undefined), function(err, data) {
	    if (err) {
		assert.fail(err, undefined, "Unexpected Error");
	    } else {
		// check the record count
		it("Returns the correct number of records", function() {
		    var totalRecords = 0;
		    Object.keys(data).map(function(key) {
			data[key].map(function(item) {
			    totalRecords += 1;
			})
		    });

		    assert.equal(totalRecords, 2, "Correct Record Count");
		});
		// check that we only get back the default delivery stream
		it("Returns a single destination", function() {
		    var keyLen = Object.keys(data).length;
		    if (keyLen > 1) {
			assert.fail(keyLen, 1, "Unexpected number of delivery streams");
		    }
		});
		it("Returns the correct delivery stream", function() {
		    // check the delivery stream name
		    Object.keys(data).map(function(key) {
			assert.equal(key, defaultDeliveryStream, "Unexpected destination");
		    });
		});
	    }
	});
    });
    describe('Routing by message attribute', function() {
	var attributeMap = {
	    "routeByMe" : {
		"Value1" : "Route1",
		"Value2" : "Route2"
	    }
	};
	var records = [];
	records.push({
	    "routeByMe" : "Value1"
	});
	records.push({
	    "routeByMe" : "Value1"
	});
	records.push({
	    "routeByMe" : "Value2"
	});
	records.push({
	    "routeByMe" : "Not mapped"
	});
	records.push({
	    "dontRouteByMe" : "Whatevs"
	});

	// base64 encode the records
	var encodedRecords = [];
	records.map(function(record) {
	    encodedRecords.push(new Buffer(JSON.stringify(record)).toString('base64'));
	});

	// prepare the routing function
	var routingFunction = router.routeByAttributeMapping.bind(undefined, attributeMap);

	// do the routing
	router.routeToDestination(defaultDeliveryStream, encodedRecords, routingFunction, function(err, data) {
	    // check no errors
	    it('Should not fail', function() {
		assert.equal(err, undefined, 'err payload');
	    });

	    // we should now have a routing table with 3 keys
	    it('Should have the right number of destinations', function() {
		assert.equal(Object.keys(data).length, 3, "correct number of destinations");
	    });

	    it('Should have mapped the first two records to Route1', function() {
		assert.equal(data["Route1"].length, 2, 'Correct Mappings');
	    });

	    it('Should have the right number of records to the default destination', function() {
		assert.equal(data[defaultDeliveryStream].length, 2, 'Correct Mappings');
	    });
	});
    });
});