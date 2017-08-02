var assert = require('assert');
var transform = require('../transformer.js')
require('../constants');

describe('Transformer Tests', function() {
    beforeEach(function() {
	delete process.env[STREAM_DATATYPE_ENV];
	delete process.env[TRANSFORMER_FUNCTION_ENV];
    });

    describe('- Verify the default transformer', function() {
	it(": Is using the default transformer", function() {
	    transform.setupTransformer(function(err, t) {
		if (err) {
		    assert.fail(err);
		} else {
		    assert.equal(t.name, "bound " + transformerRegistry.jsonToStringTransformer, " Transformer Incorrectly Set ");
		}
	    });
	});
    });

    describe('- Verify configuring the transformer', function() {
	it(": Is using the configured transformer", function() {
	    process.env[TRANSFORMER_FUNCTION_ENV] = transformerRegistry.regexToDelimiter;

	    transform.setupTransformer(function(err, t) {
		if (err) {
		    assert.fail(err);
		} else {
		    assert.equal(t.name, "bound " + transformerRegistry.regexToDelimiter, " Transformer Incorrectly Set ")
		}
	    });
	});
    });

    describe('- Verify configuring the stream datatype CSV', function() {
	it(": Is using the configured transformer", function() {
	    process.env[STREAM_DATATYPE_ENV] = 'CSV';

	    transform.setupTransformer(function(err, t) {
		if (err) {
		    assert.fail(err);
		} else {
		    assert.equal(t.name, "bound " + transformerRegistry.addNewlineTransformer, " Transformer Incorrectly Set ")
		}
	    });
	});
    });

    describe('- Verify configuring the stream datatype CSV with newlines', function() {
	it(": Is using the configured transformer", function() {
	    process.env[STREAM_DATATYPE_ENV] = 'CSV-WITH-NEWLINES';

	    transform.setupTransformer(function(err, t) {
		if (err) {
		    assert.fail(err);
		} else {
		    assert.equal(t.name, "bound " + transformerRegistry.doNothingTransformer, " Transformer Incorrectly Set ")
		}
	    });
	});
    });

    describe('- Verify configuring the stream datatype BINARY', function() {
	it(": Is using the configured transformer", function() {
	    process.env[STREAM_DATATYPE_ENV] = 'BINARY';

	    transform.setupTransformer(function(err, t) {
		if (err) {
		    assert.fail(err);
		} else {
		    assert.equal(t.name, "bound " + transformerRegistry.doNothingTransformer, " Transformer Incorrectly Set ")
		}
	    });
	});
    });
});
