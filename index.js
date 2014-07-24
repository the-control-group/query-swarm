'use strict';

var util = require('util');
var EventEmitter = require('events').EventEmitter;
var async = require('async');

module.exports = function(redis){

	var Worker = require('./lib/Worker.js')(redis);

	function QueryWorker(id, query, worker, options){

		if(!this instanceof QueryWorker)
			return new QueryWorker(id, query, worker, options);

		// the namespace used in redis for queue, cursor, lock etc
		this.id = id;

		// the user-provided query that fetches tasks
		this.query = query;

		// the worker that processes a task
		this.worker = worker;

		// the unix timestamp when a query was last run
		this.throttle = 0;

		this.options = {
			// number of
			consume: typeof options.throttle === 'number' ? options.throttle : 50,
			// minimum time between queries
			throttle: typeof options.throttle === 'number' ? options.throttle : 60000,
			// maximum concurrent workers per process
			concurrency: typeof options.concurrency === 'number' ? options.concurrency : 10,
			// the max duration a query can run before we try again
			lockTimeout: typeof options.retryDelay === 'number' ? options.retryDelay : 60000
		};

		this.workers = [];
		for (var i = 0; i < this.options.concurrency; i++) {
		    this.workers.push( new Worker(this) );
		}
	}

	util.inherits(QueryWorker, EventEmitter);

	QueryWorker.start = function(){
		this.workers.forEach(function(worker){
			worker.start();
		});
	};

	QueryWorker.stop = function(callback){
		async.map(this.workers, function(worker, callback){
			worker.stop(callback);
		}, callback);
	};

	return QueryWorker;
};
