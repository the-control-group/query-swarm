'use strict';

var util = require('util');
var EventEmitter = require('events').EventEmitter;
var async = require('async');

function OrphanedQuery(cursor, tasks) {
	this.name = 'UndeadLock';
	this.message = 'The lock expired before the query returned';
	this.cursor = cursor;
	this.tasks = tasks;
}

util.inherits(OrphanedQuery, Error);

module.exports = function(redis) {

	var Worker = require('./lib/Worker.js')(redis);

	function QuerySwarm(id, query, worker, options) {
		var self = self;

		if(!self instanceof QuerySwarm)
			return new QuerySwarm(id, query, worker, options);

		// the namespace used in redis for queue, cursor, lock etc
		self.id = id;

		// workers are currently active
		self.active = false;

		// the user-provided query that fetches tasks
		self.query = query;

		// the worker that processes a task
		self.worker = worker;

		// the unix timestamp when a query was last run
		self.throttle = 0;

		// the Worker objects that consume the queue
		self.workers = [];

		var concurrency;
		self.options = {
			// minimum time between queries
			throttle: typeof options.throttle === 'number' ? options.throttle : 60000,
			// queue length at which to trigger another query
			threshold: typeof options.threshold === 'number' ? options.threshold : 60000,
			// the max duration a query can run before we try again
			retryDelay: typeof options.retryDelay === 'number' ? options.retryDelay : 5000,
			// the max duration a query can run before we try again
			lockTimeout: typeof options.lockTimeout === 'number' ? options.lockTimeout : 20000,
			// maximum concurrent workers per process
			get concurrency() { return concurrency; },
			set concurrency(value) {
				var params = [0, Math.max(0, self.workers.length - value)];
				while(params.length - 2 < value - self.workers.length) {
					var worker = new Worker(self);
					if(self.active) worker.start();
					params.push(worker);
				}
				self.workers.splice.apply(self.workers, params);
				return concurrency = value;
			}
		};

		// set the concurrency
		self.options.concurrency = typeof options.concurrency === 'number' ? options.concurrency : 10;
	}

	util.inherits(QuerySwarm, EventEmitter);

	QuerySwarm.prototype.start = function() {
		var self = this;

		self.workers.forEach(function(worker) {
			worker.start();
		});

		self.active = true;
	};

	QuerySwarm.prototype.stop = function(callback) {
		var self = this;

		async.map(self.workers, function(worker, callback) {
			worker.stop(callback);
		}, function(err, res) {
			if(!err) self.active = false;
			return callback(err, res);
		});
	};

	QuerySwarm.prototype.populate = function() {
		var self = this;

		// throttle the frequency of populate calls
		if(self.throttle + self.options.throttle > Date.now())
			return;

		// acquire a redis lock
		var timeout = Date.now() + self.options.lockTimeout;
		redis.set(self.id + ':lock', null, 'PX', self.options.lockTimeout + 1000, 'NX', function(err, result) {

			if(result === 0)
				return;

			// get the cursor
			redis.get(self.id + ':cursor', function(err, cursor) {
				if(err)
					return self.emit('error', err, 'error getting cursor');

				// run the user-provided query
				self.query(cursor, function(err, cursor, contents) {
					if(err) {
						self.emit('error', err, 'error running the user-provided query');
						if(Date.now() >= timeout) return;
						return redis.del(self.id + ':lock', function(err) {
							if(err) self.emit('error', err, 'error releasing lock');
						});
					}

					var tasks = contents.map(JSON.stringify);

					// VERY BAD: we've exceeded our lock timeout
					if(Date.now() >= timeout)
						return self.master.emit('error', new OrphanedQuery(cursor, tasks));

					var multi = redis.multi();

					// update the redis cursor
					multi = multi.set(self.id + ':cursor', cursor);

					// add tasks to redis queue
					multi = multi.lpush.apply(multi, self.id + ':queue', tasks);

					// release the lock
					multi = multi.del(self.id + ':lock');

					multi.exec(function(err) {

						// TODO: this *could* be a bad place to have an error for different reasons
						// we need to check which command threw and act accordingly:
						//   0. new OrphanedQuery(cursor, tasks) // maybe emergency (depends on query)
						//   1. new OrphanedTasks(cursor, tasks) // definitely emergency, we have updated the cursor without pushing its tasks to the queue
						//   2. deadlock; will fix itself

						if(err)
							return self.emit('error', err, 'error populating queue');

						self.emit('populate', cursor, tasks);
					});
				});
			});
		});
	};

	return QuerySwarm;
};
