'use strict';

var util = require('util');
var EventEmitter = require('events').EventEmitter;
var Redlock = require('redlock');
var async = require('async');

function OrphanedQuery(cursor, tasks) {
	this.name = 'UndeadLock';
	this.message = 'The lock expired before the query returned.';
	this.cursor = cursor;
	this.tasks = tasks;
}

util.inherits(OrphanedQuery, Error);

module.exports = function(redis) {

	var Worker = require('./Worker.js')(redis);

	function QuerySwarm(id, query, worker, options) {
		var self = this;

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

		// the Worker objects that consume the queue
		self.workers = [];

		var concurrency;
		options = options || {};
		self.options = {
			// minimum time between queries
			throttle: typeof options.throttle === 'number' ? options.throttle : 10000,
			// queue length at which to trigger another query
			threshold: typeof options.threshold === 'number' ? options.threshold : 10,
			// the duration a worker waits between polling an empty queue
			retryDelay: typeof options.retryDelay === 'number' ? options.retryDelay : 500,
			// the max duration a query can run before we try again
			lockTimeout: typeof options.lockTimeout === 'number' ? options.lockTimeout : 20000,
			// the max number of attempts to process a task
			maxProcessingRetries: typeof options.maxProcessingRetries === 'number' ? options.maxProcessingRetries : 0,
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
				return (concurrency = value);
			}
		};

		// set the concurrency
		self.options.concurrency = typeof options.concurrency === 'number' ? options.concurrency : 10;

		// create instance or redlock that won't retry for a lock
		self.redlock = new Redlock([redis], { retryCount: 0 });
		self.redlock.on('clientError', function(err) {
			self.emit('clientError', err);
		});
	}

	util.inherits(QuerySwarm, EventEmitter);

	QuerySwarm.prototype.start = function() {
		var self = this;

		self.workers.forEach(function(worker) {
			worker.start();
		});

		self.active = true;
		self.emit('started');
		return self;
	};

	QuerySwarm.prototype.stop = function(callback) {
		var self = this;

		async.map(self.workers, function(worker, callback) {
			worker.stop(callback);
		}, function(err, res) {
			if(!err) self.active = false;
			self.emit('stopped', err, res);
			if(callback instanceof Function) callback(err, res);
		});

		return self;
	};

	QuerySwarm.prototype.destroy = function(callback) {
		var self = this;
		self.stop(function(err, res){

			// TODO: get the lock, cursor, queue, processing, & deadletter; write to file??? emit??? log???

			redis.del(self.id + ':lock', self.id + ':throttle', self.id + ':cursor', self.id + ':queue', self.id + ':processing', self.id + ':deadletter', callback);
			self.emit('destroyed', err, res);
		});
		return self;
	};

	QuerySwarm.prototype.getCursor = function(callback) {
		return redis.get(this.id + ':cursor', function(err, cursor) {
			return callback(err, JSON.parse(cursor));
		});
	};

	// TODO either make sure the swarm is stopped or require caller to provide expected value to be updated
	QuerySwarm.prototype.setCursor = function(value, callback) {
		return redis.set(this.id + ':cursor', value, function(err) {
			return callback(err);
		});
	};

	QuerySwarm.prototype.getThrottle = function(callback) {
		return redis.pttl(this.id + ':throttle', function(err, pttl) {
			return callback(err, Date.now() + Math.max(pttl,0));
		});
	};

	QuerySwarm.prototype.getQueue = function(callback) {
		return redis.lrange(this.id + ':queue', 0, -1, function(err, queue) {
			return callback(err, queue.map(function(q){ return JSON.parse(q); }));
		});
	};

	QuerySwarm.prototype.getQueueLength = function(callback) {
		return redis.llen(this.id + ':queue', function(err, llen) {
			return callback(err, llen);
		});
	};

	QuerySwarm.prototype.getProcessing = function(callback) {
		return redis.lrange(this.id + ':processing', 0, -1, function(err, processing) {
			return callback(err, processing.map(function(q){ return JSON.parse(q); }));
		});
	};

	QuerySwarm.prototype.getProcessingLength = function(callback) {
		return redis.llen(this.id + ':processing', function(err, llen) {
			return callback(err, llen);
		});
	};

	QuerySwarm.prototype.populate = function() {
		var self = this;

		// throttle the frequency of populate calls
		return self.getThrottle(function(err, throttle) {
			if (err)
				return;

			if(self.active === true && throttle > Date.now())
				return;

			// defer to the next tick
			setImmediate(function(){
				if(self.active !== true)
					return;

				// acquire a redis lock
				self.redlock.lock(self.id + ':lock', self.options.lockTimeout, function(err, lock) {
					if(err || !lock)
						return;

					// acquire the distributed throttle
					self.redlock.lock(self.id + ':throttle', self.options.throttle, function(err, throttle) {
						if(err || !throttle) {
							lock.unlock();
							return;
						}

						self.throttle = Date.now() + self.options.throttle;

						// get the cursor
						self.getCursor(function(err, cursor) {
							if(err) {
								lock.unlock();
								return self.emit('error', err, 'error getting cursor');
							}

							// run the user-provided query
							self.query(cursor, function(err, cursor, contents) {
								if(err) {
									lock.unlock();
									return self.emit('error', err, 'error running the user-provided query');
								}

								var tasks = (Array.isArray(contents) && contents.length > 0) ? contents.map(JSON.stringify) : [];

								// VERY BAD: we've exceeded our lock timeout
								if(Date.now() >= lock.expiration)
									return self.emit('error', new OrphanedQuery(cursor, tasks));

								// TODO: extend the lock if its expiration has fallen below a configurable buffer

								var multi = redis.multi();
								
								// update the redis cursor
								multi = multi.set(self.id + ':cursor', JSON.stringify(cursor));

								// add tasks to redis queue
								if(tasks.length > 0) {
									var params = tasks.slice();
									while (params.length > 0) {
										var args = params.splice(0,10000)
										args.unshift(self.id + ':queue');
										multi = multi.lpush.apply(multi, args);
									}
								}

								multi.exec(function(err) {

									// release the lock
									lock.unlock();

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
				});
			});
		});
		return self;
	};

	return QuerySwarm;
};
