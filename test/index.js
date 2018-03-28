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
};

after(function(done) {
	redis.del('QuerySwarm:test:cursor', done);
});

before(function(done) {
	redis.scan('0',function(err, results) {
		if (err) return done(err);
		if (results[1].length !== 0) return done(new Error('Redis is not empty'));
		done();
	})
});

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
		swarm.once('stopped',function(){
			// the swarm should query once
			assert.equal(q, 1);

			// and consume the first 10 jobs before issuing a stop command;
			// could have processed 10 or 11 jobs, because we have 2 workers
			assert.isAbove(w, 9);
			assert.isBelow(w, 12);
			done();
		});
		swarm.start();
	});

	it('should start and stop when commanded', function(done){
		swarm.start();
		swarm.once('stopped',function(){

			// the swarm should query one more time
			assert.equal(q, 2);

			// and consume the next 10 jobs before issuing a stop command;
			// could have processed 20 or 21 jobs, because we have 2 workers
			assert.isAbove(w, 19);
			assert.isBelow(w, 22);
			done();
		});
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
		{
			throttle: 10,
			threshold: 4,
			retryDelay: 50,
			lockTimeout: 200,
			concurrency: 2
		}
	);

	// increment the # of errors
	swarm.on('error', function(err, msg){errs++;});

	it('should continue processing after a task fails', function(done){
		swarm.destroy(function(err){
			if(err) return done(err);
			swarm.once('stopped', function(){
				assert.equal(errs, 2);

				// the swarm should query one more time
				assert.equal(q, 2);

				// and consume the next 10 jobs before issuing a stop command;
				// could have processed 20 or 21 jobs, because we have 2 workers
				assert.isAbove(w, 19);
				assert.isBelow(w, 22);

				done();
			});
			swarm.start();
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

describe('Requeue', function(){
	it('should requeue up to maxProcessingRetries', function(done) {
		var r = 0;
		var d = 0;
		var e = 0;
		var q = 0;
		var w = 0;
		var tasks = [1,1];
		var expectedWorkerRuns = tasks.length * 2;
		var expectedRequeues = tasks.length;
		var expectedDeadletters = tasks.length;
		var expectedErrors = tasks.length * 2;
		var swarm = new QuerySwarm(
			'QuerySwarm:test:'+this.test.fullTitle(),
			function(cursor, callback) {
				q++;
				callback(null, cursor, tasks.splice(0,1));
			},
			function(task, callback) {
				w++;
				callback(new Error('test'));
			},
			{
				throttle: 10,
				threshold: 4,
				retryDelay: 50,
				lockTimeout: 200,
				concurrency: 2,
				maxProcessingRetries: 1,
			}
		);
		swarm.on('error', function(){
			e++;
		});
		swarm.on('requeue', function(task) {
			r++;
			assert.equal(task, 1);
		});
		swarm.on('deadletter', function(task) {
			d++;
			assert.equal(task, 1);
		});
		swarm.start();
		setTimeout(function(){
			swarm.destroy(function(){
				assert.equal(w, expectedWorkerRuns);
				assert.equal(r, expectedRequeues);
				assert.equal(d, expectedDeadletters);
				assert.equal(e, expectedErrors);
				done();
			});
		},500)
	});
	it('should requeue up to maxProcessingRetries unless the worker specifies forceDeadletter', function(done) {
		var r = 0;
		var d = 0;
		var e = 0;
		var q = 0;
		var w = 0;
		var tasks = [1,1];
		var expectedWorkerRuns = tasks.length;
		var expectedRequeues = 0;
		var expectedDeadletters = tasks.length;
		var expectedErrors = tasks.length;
		var swarm = new QuerySwarm(
			'QuerySwarm:test:'+this.test.fullTitle(),
			function(cursor, callback) {
				q++;
				callback(null, cursor, tasks.splice(0,1));
			},
			function(task, callback) {
				w++;
				callback(new Error('test'), null, true);
			},
			{
				throttle: 10,
				threshold: 4,
				retryDelay: 50,
				lockTimeout: 200,
				concurrency: 2,
				maxProcessingRetries: 1,
			}
		);
		swarm.on('error', function(){
			e++;
		});
		swarm.on('requeue', function(task) {
			r++;
			assert.equal(task, 1);
		});
		swarm.on('deadletter', function(task) {
			d++;
			assert.equal(task, 1);
		})
		swarm.start();
		setTimeout(function(){
			swarm.destroy(function(){
				assert.equal(w, expectedWorkerRuns);
				assert.equal(r, expectedRequeues);
				assert.equal(d, expectedDeadletters);
				assert.equal(e, expectedErrors);
				done();
			});
		},500)
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
				assert.ok(false, 'the worker should never be run.');
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

describe('Populate', function() {
	it('should not crash due to "RangeError: Maximum call stack size exceeded" when a large result set is returned', function(done) {
		var swarm = new QuerySwarm(
			'QuerySwarm:test:'+this.test.fullTitle(),
			function(cursor, callback) {
				callback(null, null, new Array(1000000));
				swarm.stop();
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
		swarm.start();
		swarm.on('populate', function(cursor, tasks){
			assert.equal(tasks.length,1000000);
			done();
		});
	});
});
