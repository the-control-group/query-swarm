'use strict';

var util = require('util');
var EventEmitter = require('events').EventEmitter;
var async = require('async');

module.exports = function(redis){

	var Worker = require('./lib/Worker.js')(redis);

	function QuerySwarm(id, query, worker, options){
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
			lockTimeout: typeof options.retryDelay === 'number' ? options.retryDelay : 5000,
			// the max duration a query can run before we try again
			lockTimeout: typeof options.lockTimeout === 'number' ? options.lockTimeout : 20000,
			// maximum concurrent workers per process
			get concurrency() { return concurrency; },
			set concurrency(value) {
				var params = [0, Math.max(0, workers.length - value)];
				while(params.length - 2 < value - workers.length) {
					var worker = new Worker(self);
					if(self.active) worker.start();
					params.push(worker);
				}
				workers.splice.apply(workers, params)
				return concurrency = value;
			}
		};

		// set the concurrency
		self.options.concurrency = typeof options.concurrency === 'number' ? options.concurrency : 10;
	}

	util.inherits(QuerySwarm, EventEmitter);

	QuerySwarm.prototype.start = function(){
		var self = this;

		self.workers.forEach(function(worker){
			worker.start();
		});

		self.active = true;
	};

	QuerySwarm.prototype.stop = function(callback){
		var self = this;

		async.map(self.workers, function(worker, callback){
			worker.stop(callback);
		}, function(err, res){
			if(!err) self.active = false;
			return callback(err, res);
		});
	};

	QuerySwarm.prototype.populate = function(){
		var self = this;

		// throttle the frequency of populate calls
		if(self.throttle + self.options.throttle > Date.now())
			return;

		/*************************************************************************************
		 * This is heavily inspired by the work of Rakesh Pai <rakeshpai@errorception.com>
		 * https://github.com/errorception/redis-lock
		 *************************************************************************************/

		redis.setnx(self.id + ':lock', Date.now() + self.options.lockTimeout + 1, function(err, result) {
			if(err)
				return self.emit('error', 'error acquiring lock', err);

			if(result !== 0)
				return next();

			// check for deadlock
			return redis.get(self.id + ':lock', function(err, oldTimeout) {
				if(err)
					return self.emit('error', 'error checking for deadlock', err);

				// the lock was removed after calling .setnx but before calling .get
				// the queue likely has new tasks, so let's try to consume it
				if(oldTimeout === null)
					return;

				oldTimeout = parseFloat(oldTimeout);

				// lock is valid
				if(oldTimeout > Date.now())
					return;

				// recover from deadlock
				var timeout = (Date.now() + self.options.lockTimeout + 1);
				redis.getset(self.id + ':lock', timeout, function(err, lastTimeout) {
					if(err)
						return self.emit('error', 'error recovering from deadlock', err);

					// the deadlock was already recovered
					if(lastTimeout != oldTimeout)
						return;

					// we have the lock
					return next();
				});
			});
		});

		function next(){

			// get the cursor
			redis.get(self.id + ':cursor', function(err, cursor){
				if(err)
					return self.emit('error', 'error getting cursor', err);

				// run the user-provided query
				self.query(cursor, function(err, cursor, contents){
					if(err) {
						self.emit('error', 'error running the user-provided query', err);
						return redis.del(self.id + ':lock', function(err){
							return if(err) self.emit('error', 'error releasing lock', err);
						});
					}

					var tasks = contents.map(JSON.stringify);

					var multi = redis.multi();

					// release the lock
					multi = multi.del(self.id + ':lock');

					// add tasks to redis queue
					multi = multi.lpush.apply(multi, self.id + ':queue', tasks);

					// update the redis cursor
					multi = multi.set(self.id + ':cursor', cursor);

					multi.exec(function(err){

						// TODO: this *could* be a bad place to have an error. we need to check which command threw and act accordingly
						if(err)
							return self.emit('error', 'error populating queue', err);

						self.emit('populate', cursor, tasks);
					});
				});
			});
		}
	};

	return QuerySwarm;
};
