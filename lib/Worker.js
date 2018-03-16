'use strict';

var util = require('util');

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

	function Worker(master){
		this.master = master;

		// this worker is either processing a task or waiting for one
		this.active = false;

		// null || setTimeoutObject from waiting job
		this.immediate = null;
		this.timeout = null;
	}

	Worker.prototype.start = function(){
		var self = this;

		// make sure we aren't already running
		if(self.active)
			return;

		delete self.next;
		self.active = true;
		self.next('consume');
	};

	Worker.prototype.stop = function(callback){
		var self = this;

		// make sure we aren't already stopped or stopping
		if(!self.active || self.next !== Worker.prototype.next)
			return callback();

		// if we have a timeout or immediate set, just clear it and return
		if(self.timeout !== null || self.immediate !== null){
			clearImmediate(self.immediate);
			clearTimeout(self.timeout);
			self.immediate = null;
			self.timeout = null;
			self.active = false;
			return callback();
		}

		// we're mid-job; gracefully exit when it's done
		return (self.next = function(){
			delete self.next;
			self.active = false;
			callback();
		});
	};

	Worker.prototype.destroy = function(callback){
		var self = this;
		self.stop(function(){
			// TODO: destroy redis keys and emit their values
			callback();
		});
	};

	Worker.prototype.next = function(method, delay){
		var self = this;

		// we're done with the timeout, so remove it
		clearImmediate(self.immediate);
		clearTimeout(self.timeout);
		self.immediate = null;
		self.timeout = null;

		// this should never actually happen
		if(self.active === false)
			return;

		if(delay === false)
			return self[method]();

		if(delay)
			return (self.timeout = setTimeout(self.next.bind(self), delay, method));

		return (self.immediate = setImmediate(self.next.bind(self), method, false));
	};

	Worker.prototype.consume = function(){
		var self = this;

		redis.multi()

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

				// run the user-provided worker
				self.master.worker(JSON.parse(task), function(err, result, forceDeadletter){
					if(err) {
						self.master.emit('error', err, 'error running the user-proviced worker');

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
					setTimeout(self.deadletter.bind(self), self.master.options.retryDelay, task);
				}

				self.master.emit('deadletter', task);
				return self.next('consume');
			});
	};

	Worker.prototype.acknowledge = function(task){
		var self = this;

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
					setTimeout(self.acknowledge.bind(self), self.master.options.retryDelay, task);
				}

				self.master.emit('acknowledge', task);
				return self.next('consume');
			});
	};

	Worker.prototype.requeue = function(task){
		var self = this;

		redis.zscore(self.master.id + ':retries', task, function(err, score) {
			if (err) {
				self.master.emit('error', err, 'error requeueing task');
				self.master.emit('error', new RequeueError(task));
				// save a reference to this timer somewhere so it can be cleared on stop?
				return setTimeout(self.requeue.bind(self), self.master.options.retryDelay, task);
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
						return setTimeout(self.requeue.bind(self), self.master.options.retryDelay, task);
					}
					
					self.master.emit('requeue', task);
					return self.next('consume');
				});
		});
	};

	return Worker;
};
