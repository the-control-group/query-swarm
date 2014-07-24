'use strict';

module.exports = function(redis) {

	function Worker(master){
		this.master = master;

		// this worker is either processing a task or waiting for one
		this.active = false;

		// null || setTimeoutObject from waiting job
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

		// if we have a timout set, just clear it and return
		if(self.timeout){
			clearTimeout(self.timeout);
			self.timeout = null;
			self.active = false;
			return callback();
		}

		// we're mid-job; gracefully exit when it's done
		return self.next = function(){
			delete self.next;
			self.active = false;
			callback();
		};
	};

	Worker.prototype.next = function(method, delay){
		var self = this;

		// we're done with the timeout, so remove it
		clearTimeout(self.timeout);
		self.timeout = null;

		if(delay)
			return self.timeout = setTimeout(self.next.bind(self), delay, method);

		return self[method]();
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
					self.master.emit('error', 'error consuming queue', err);
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
				self.master.worker(JSON.parse(task), function(err, result){
					if(err) {
						self.master.emit('error', 'error running the user-proviced worker', err);
						return self.deadletter(task);
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
			.lrem(self.master.id + ':processing', task)

			.exec(function(err){
				if(err){
					// TODO: retry instead of jumping back into consume
					self.master.emit('error', 'error deadlettering task', err, task);
					return self.next('consume', self.master.options.retryDelay);
				}

				self.master.emit('deadletter', task);
				return self.next('consume');
			});
	};

	Worker.prototype.acknowledge = function(task){
		var self = this;

		// remove from processing list
		redis.lrem(self.master.id + ':processing', task, function(err){
			if(err){
				// TODO: retry instead of jumping back into consume
				self.master.emit('error', 'error acknowledging task', err, task);
				return self.next('consume', self.master.options.retryDelay);
			}

			self.master.emit('acknowledge', task);
			return self.next('consume');
		});
	};
};
