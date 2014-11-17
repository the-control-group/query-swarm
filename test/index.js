'use strict';

var assert = require('chai').assert;
var redis = require('redis').createClient();
var QuerySwarm = require('../lib/QuerySwarm.js')(redis);

var db = [];
for (var i = 0, l = 100; i < l; i++) {
	db.push(i);
}

// TODO: test each config option seperately!!!
var opts = {
	throttle: 10,
	threshold: 4,
	retryDelay: 50,
	lockTimeout: 200,
	concurrency: 2
}

describe('Start/Stop', function(){
	var q = 0;
	var w = 0;
	var swarm = new QuerySwarm(
		'QuerySwarm:test',
		function(cursor, callback) {
			q++;
			cursor = cursor || 0;
			var res = [];
			for (var i = 0, l = 15; i < l; i++) {
				res.push(cursor + i);
			}
			callback(null, cursor+15, res);
		},
		function(task, callback) {
			w++;
			// stop on 10, 20, 30...
			if(w % 10 === 0) swarm.stop();
			callback(null, {});
		},
		opts
	);

	it('should not start when created', function(done){
		setTimeout(function(){
			assert.equal(q, 0);
			assert.equal(w, 0);
			done();
		}, 100);
	});

	it('should not start when destroyed', function(done){
		swarm.destroy(function(err){
			if(err) return done(err);
			setTimeout(function(){
				assert.equal(q, 0);
				assert.equal(w, 0);
				done();
			}, 100);
		});
	});

	it('should start and stop when commanded', function(done){
		swarm.start();
		setTimeout(function(){
			assert.equal(q, 1);
			assert.ok(w > 9 && w < 12); // can be 10 or 11, because we have 2 workers
			done();
		}, 500);
	});

	it('should start and stop when commanded', function(done){
		swarm.start();
		setTimeout(function(){
			assert.equal(q, 2);
			assert.ok(w > 19 && w < 22); // can be 20 or 21, because we have 2 workers
			done();
		}, 500);
	});

	it('should not move successful tasks to the deadletter list', function(done){
		redis.llen('QuerySwarm:test:deadletter', function(err, res){
			if(err) return done(err);
			assert.equal(res, 0);
			done();
		});
	});

	it('should not leave tasks in the processing list', function(done){
		redis.llen('QuerySwarm:test:processing', function(err, res){
			if(err) return done(err);
			assert.equal(res, 0);
			done();
		});
	});

	it('should leave unprocessed tasks in the queue', function(done){
		redis.llen('QuerySwarm:test:queue', function(err, res){
			if(err) return done(err);
			assert.equal(res, q * 15 - w);
			done();
		});
	});

	it('should leave the cursor in the correct place', function(done){
		redis.get('QuerySwarm:test:cursor', function(err, res){
			if(err) return done(err);
			assert.equal(JSON.parse(res), q * 15);
			done();
		});
	});
});


describe('Deadletter', function(){
	var q = 0;
	var w = 0;
	var errs = 0;

	var swarm = new QuerySwarm(
		'QuerySwarm:test',
		function(cursor, callback) {
			q++;
			cursor = cursor || 0;
			callback(null, cursor+15, [1,1,1,1,1,1,1,1,1,1,1,1,1,1,1]);
		},
		function(task, callback) {
			w++;

			// stop on 20, 40, 60...
			if(w % 20 === 0) swarm.stop();

			// deadletter 5, 15, 25...
			if((w+5) % 10 === 0)
				return callback(new Error());

			callback(null, {});
		},
		opts
	);

	// increment the # of errors
	swarm.on('error', function(err, msg){errs++;});

	it('should continue processing after a task fails', function(done){
		swarm.destroy(function(err){
			if(err) return done(err);
			swarm.start();
			setTimeout(function(){
				assert.equal(errs, 2);
				assert.equal(q, 2);
				assert.ok(w > 19 && w < 22); // can be 20 or 21, because we have 2 workers
				done();
			}, 100);
		});
	});

	it('should move failed tasks to the deadletter list', function(done){
		redis.llen('QuerySwarm:test:deadletter', function(err, res){
			if(err) return done(err);
			assert.equal(res, 2);
			done();
		});
	});

	it('should not leave tasks in the processing list', function(done){
		redis.llen('QuerySwarm:test:processing', function(err, res){
			if(err) return done(err);
			assert.equal(res, 0);
			done();
		});
	});

	it('should leave unprocessed tasks in the queue', function(done){
		redis.llen('QuerySwarm:test:queue', function(err, res){
			if(err) return done(err);
			assert.equal(res, q * 15 - w);
			done();
		});
	});

	it('should leave the cursor in the correct place', function(done){
		redis.get('QuerySwarm:test:cursor', function(err, res){
			if(err) return done(err);
			assert.equal(JSON.parse(res), q * 15);
			done();
		});
	});

});

describe('Errors', function(){

	it('should emit an error if there is a problem with the query', function(done){
		var i = 0;
		var swarm = new QuerySwarm(
			'QuerySwarm:test',
			function(cursor, callback) {
				assert.equal(i, 0, 'the query should have only run once.');
				i++;
				callback(new Error('error1'));
			},
			function(task, callback) {
				assert.ok(false, 'the worker should never be run.')
			},
			opts
		);
		swarm.destroy(function(){
			swarm.on('error', function(err, message){
				assert.equal(err.message, 'error1');
				swarm.stop(done);
			});
			swarm.start();
		});
	});

	it('should emit an error if there is a problem with the worker', function(done){
		var i = 0;
		var stopped = false;
		var swarm = new QuerySwarm(
			'QuerySwarm:test',
			function(cursor, callback) {
				assert.equal(i, 0, 'the query should have only run once.');
				i++;
				callback(null, cursor+1, [1,1,1,1,1,1,1,1,1,1]);
			},
			function(task, callback) {
				callback(new Error('error2'), {});
			},
			opts
		);
		swarm.destroy(function(){
			swarm.on('error', function(err, message){
				assert.equal(err.message, 'error2');
				if(!stopped)
					swarm.stop(done);
				stopped = true;
			});
			swarm.start();
		});
	});
});

describe('Events', function(){
	var dump = [];
	var queryCount = 0;
	var workerCount = 0;
	var populateArr = [];
	var consumeArr = [];

	it('should process each task exactly once', function(done){
		this.timeout(3000);

		var swarm = new QuerySwarm(
			'QuerySwarm:test',
			function(cursor, callback) {
				queryCount++;
				var newCursor = Math.min(db.length, cursor+10);
				callback(null, newCursor, db.slice(cursor, newCursor));
			},
			function(task, callback) {
				workerCount++;
				dump.push(task);
				if(dump.length == db.length) {
					assert.sameMembers(dump, db);
					swarm.stop(done);
				}
				setTimeout(callback, 10, null, dump.length);
			},
			opts
		);
		swarm.destroy(function(){
			swarm.on('populate', function(cursor, tasks){
				populateArr.push(tasks.length);
			});

			swarm.on('consume', function(task){
				consumeArr.push(task);
			});

			swarm.start();
		});
	});

	it('should emit a consume event each time a worker completes', function(){
		assert.lengthOf(consumeArr, workerCount);
		assert.sameMembers(consumeArr.map(JSON.parse), db);
	});

	it('should emit a populate event each time a query completes', function(){
		assert.lengthOf(populateArr, queryCount);
		assert.equal(populateArr.reduce(function(a,b){return a+b;}), db.length);
	});
});
