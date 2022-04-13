'use strict';

var util = require('util');
var EventEmitter = require('events').EventEmitter;
var assert = require('assert');

function DeadletterError(task) {
	this.name = 'DeadletterError';
	this.message = 'Unable to deadletter task. Retrying.';
	this.task = task;
}

util.inherits(DeadletterError, Error);

function AcknowledgeError(task) {
	this.name = 'AcknowledgeError';
	this.message = 'Unable to acknowledge task. Retrying.';
	this.task = task;
}

util.inherits(AcknowledgeError, Error);

function RequeueError(task) {
	this.name = 'RequeueError';
	this.message = 'Unable to requeue task. Retrying.';
	this.task = task;
}

util.inherits(RequeueError, Error);

module.exports = function(redis) {

	function Worker(master, id){
		this.master = master;

		// id of this worker
		this.id = id;

		// this worker is either processing a task or waiting for one
		this.active = false;

		// null || setTimeout reference from waiting job
		this.immediate = null;

		// null || setImmediate reference from waiting job
		this.timeout = null;

		// null || info about the status of the worker
		this.job = null;

		// whether or not the worker is in the process of stopping
		this.stopping = false;
	}

	util.inherits(Worker, EventEmitter);

	Worker.prototype.start = function(){
		var self = this;

		// make sure we aren't already running
		if(self.active)
			return;

		// what if we are currently stopping?
		if (self.stopping) return self.once('stopped', function() {
			delete self.next;
			self.active = true;
			self.stopping = false;
			self.next('consume');
		});

		delete self.next;
		self.active = true;
		self.stopping = false;
		self.next('consume');
	};

	Worker.prototype.stop = function(callback){
		var self = this;

		// we're already stopped
		if(!self.active) {
			return callback();
		}

		// we're already stopping
		if(self.stopping) {
			return self.once('stopped', callback);
		}

		// if we have a timeout or immediate set, just clear it and return
		// this means the worker is doing nothing at the moment
		if(self.hasTimeout()){
			self.clearTimeouts();
			self.active = false;
			self.emit('stopped', self.id);
			return callback();
		}

		self.stopping = true;

		// we're mid-job; gracefully exit when it's done
		return (self.next = function(){
			delete self.next;
			self.active = false;
			self.stopping = false;
			self.emit('stopped', self.id);
			return callback();
		});
	};

	Worker.prototype.next = function(method, delay, ...args){
		var self = this;

		assert.ok(self.active, 'Worker is not active');

		self._current = null;

		if(delay === false) {
			// we're done with the timeout, so remove it
			self.clearTimeouts();
			return self[method].apply(self, args);
		}

		assert.equal(self.hasTimeout(), false, 'Worker already has timeout set');
		assert.ok(typeof self[method] === 'function', 'Worker['+method+'] is not a function');

		self.job = {
			method: method,
			args: args,
			status: 'waiting',
			scheduled: delay ? Date.now() + delay : Date.now(),
		};

		if(delay)
			return (self.timeout = setTimeout(self.next.bind(self), delay, method, false, ...args));

		return (self.immediate = setImmediate(self.next.bind(self), method, false, ...args));
	};

	Worker.prototype.hasTimeout = function() {
		var self = this;
		return self.timeout !== null || self.immediate !== null;
	};

	Worker.prototype.clearTimeouts = function() {
		var self = this;

		if (self.hasTimeout()) {
			clearImmediate(self.immediate);
			clearTimeout(self.timeout);
			self.immediate = null;
			self.timeout = null;
			self.job = null;
		}
	};

	Worker.prototype.consume = function(){
		var self = this;

		self.job = {
			method: 'consume',
			started: Date.now(),
			status: 'Getting task',
		};

		return redis.multi()

			// get a task from the queue
			.rpoplpush(self.master.id + ':queue', self.master.id + ':processing')

			// get the queue length
			.llen(self.master.id + ':queue')

			.exec(function(err, replies){

				if(err) {
					self.master.emit('error', err, 'error consuming queue');
					return self.next('consume', self.master.options.retryDelay);
				}

				var task = replies[0];
				var length = replies[1];

				// have the master populate the queue
				if(length < self.master.options.threshold)
					self.master.populate();

				if(task === null)
					return self.next('consume', self.master.options.retryDelay);

				self.job.status = 'Running user-provided worker';
				self.job.task = JSON.parse(task);

				// run the user-provided worker
				return self.master.worker(JSON.parse(task), function(err, result, forceDeadletter){
					if(err) {
						self.master.emit('error', err, 'error running the user-provided worker');

						// checking maxProcessingRetries here prevents an unnecessary zsore in the requeue method if retries are not enabled
						return self.master.options.maxProcessingRetries && !forceDeadletter ? self.requeue(task) : self.deadletter(task);
					}

					self.master.emit('consume', task, result);
					return self.acknowledge(task);
				});
			});
	};



	Worker.prototype.deadletter = function(task){
		var self = this;

		self.job = {
			method: 'deadletter',
			started: Date.now(),
			status: 'Sending task to deadletter',
		};

		redis.multi()
			// add to the deadletter list
			.lpush(self.master.id + ':deadletter', task)

			// remove from processing list
			.lrem(self.master.id + ':processing', 1, task)

			// remove from retry list
			.zrem(self.master.id + ':retries', task)

			.exec(function(err){
				if(err){

					// TODO: differentiate between errors in lpush and lrem

					self.master.emit('error', err, 'error deadlettering task');
					self.master.emit('error', new DeadletterError(task));

					// save a reference to this timer somewhere so it can be cleared on stop?
					// return self.timeout = setTimeout(self.deadletter.bind(self), self.master.options.retryDelay, task);
					return self.next('deadletter', self.master.options.retryDelay, task);
				}

				self.master.emit('deadletter', task);
				return self.next('consume');
			});
	};

	Worker.prototype.acknowledge = function(task){
		var self = this;

		self.job = {
			method: 'acknowledge',
			started: Date.now(),
			status: 'Marking task as complete',
		};

		// remove from processing list
		redis.multi()
			// remove from processing list
			.lrem(self.master.id + ':processing', 1, task)

			// remove from retry list
			.zrem(self.master.id + ':retries', task)

			.exec(function(err){
				if(err){
					self.master.emit('error', err, 'error acknowledging task');
					self.master.emit('error', new AcknowledgeError(task));

					// save a reference to this timer somewhere so it can be cleared on stop?
					// return self.timeout = setTimeout(self.acknowledge.bind(self), self.master.options.retryDelay, task);
					return self.next('acknowledge', self.master.options.retryDelay, task);
				}

				self.master.emit('acknowledge', task);
				return self.next('consume');
			});
	};

	Worker.prototype.requeue = function(task){
		var self = this;

		self.job = {
			method: 'requeue',
			started: Date.now(),
			status: 'Sending task back to queue',
		};

		redis.zscore(self.master.id + ':retries', task, function(err, score) {
			if (err) {
				self.master.emit('error', err, 'error requeueing task');
				self.master.emit('error', new RequeueError(task));
				// save a reference to this timer somewhere so it can be cleared on stop?
				// return self.timeout = setTimeout(self.requeue.bind(self), self.master.options.retryDelay, task);
				return self.next('requeue', self.master.options.retryDelay, task);
			}

			if (score >= self.master.options.maxProcessingRetries) {
				return self.deadletter(task);
			}

			// move from processing list back to queue and increment retry score
			redis.multi()
				.lrem(self.master.id + ':processing', 1, task)
				.lpush(self.master.id + ':queue', task)
				.zincrby(self.master.id + ':retries', 1, task)
				.exec(function(err, replies) {
					if(err){
						self.master.emit('error', err, 'error requeueing task');
						self.master.emit('error', new RequeueError(task));
						// save a reference to this timer somewhere so it can be cleared on stop?
						// return self.timeout = setTimeout(self.requeue.bind(self), self.master.options.retryDelay, task);
						return self.next('requeue', self.master.options.retryDelay, task);
					}
					
					self.master.emit('requeue', task);
					return self.next('consume');
				});
		});
	};

	return Worker;
};
