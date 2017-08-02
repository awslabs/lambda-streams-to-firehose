var assert = require('assert');
var router = require('../router.js')
var transformer = require('../transformer.js')
require('../constants');

var jsonFragment = '{"key1":"value1","key2":"value2","key3":567}';
var csvFragment = '"ABC"|12345|22.589|"This is a the product description"';
describe('Input Data Tests', function() {
    describe('Verify the transformer does not add escape sequence to strings with double quotes', function() {
	var records = [ jsonFragment ];
	transformer.addNewlineTransformer(records, function(err, data) {
	    if (err) {
		assert.fail(err, undefined, "Unexpected Error");
	    } else {
		it("Does not add escape sequence", function() {
		    assert.equal(data.toString(targetEncoding), records[0] + "\n", " The data got modified ")
		});
	    }
	});
    });
    describe('Verify the transformer does not modify non JSON data', function() {
	var records = [ csvFragment ];
	transformer.addNewlineTransformer(records, function(err, data) {
	    if (err) {
		assert.fail(err, undefined, "Unexpected Error");
	    } else {
		it("Verify that the CSV data is right", function() {
		    assert.equal(data.toString(targetEncoding), records[0] + "\n", " The CSV data got modified")
		});
	    }
	});
    });
});
