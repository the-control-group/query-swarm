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

		// get a task from the queue
		redis.rpoplpush(self.master.id + ':queue', self.master.id + ':processing', function(err, task){
			if(err) {
				self.master.emit('error', 'error consuming queue', err);
				return self.next('consume', self.master.options.retryDelay);
			}

			if(task === null)
				return self.next('populate');

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

	Worker.prototype.populate = function(){
		var self = this;

		// throttle the frequency of populate calls
		if(self.master.throttle + self.master.options.throttle > Date.now())
			return self.next('consume', self.master.options.retryDelay);

		/*************************************************************************************
		 * This is heavily inspired by the work of Rakesh Pai <rakeshpai@errorception.com>
		 * https://github.com/errorception/redis-lock
		 *************************************************************************************/

		redis.setnx(self.master.id + ':lock', Date.now() + self.master.options.lockTimeout + 1, function(err, result) {
			if(err){
				self.master.emit('error', 'error acquiring lock', err);
				return self.next('consume', self.master.options.retryDelay);
			}

			if(result !== 0)
				return next();

			// check for deadlock
			return redis.get(self.master.id + ':lock', function(err, oldTimeout) {
				if(err){
					self.master.emit('error', 'error checking for deadlock', err);
					return self.next('consume', self.master.options.retryDelay);
				}

				// the lock was removed after calling .setnx but before calling .get
				// the queue likely has new tasks, so let's try to consume it
				if(oldTimeout === null)
					return self.next('consume', self.master.options.retryDelay);

				oldTimeout = parseFloat(oldTimeout);

				// lock is valid
				if(oldTimeout > Date.now())
					return self.next('consume', self.master.options.retryDelay);

				// recover from deadlock
				var timeout = (Date.now() + self.master.options.lockTimeout + 1);
				redis.getset(self.master.id + ':lock', timeout, function(err, lastTimeout) {
					if(err){
						self.master.emit('error', 'error recovering from deadlock', err);
						return self.next('consume', self.master.options.retryDelay);
					}

					// the deadlock was already recovered
					if(lastTimeout != oldTimeout)
						return self.next('consume', self.master.options.retryDelay);

					// we have the lock
					return next();
				});
			});
		});

		function next(){

			// get the cursor
			redis.get(self.master.id + ':cursor', function(err, cursor){
				if(err){
					self.master.emit('error', 'error getting cursor', err);
					return self.next('consume', self.master.options.retryDelay);
				}

				// run the user-provided query
				self.master.query(cursor, function(err, cursor, contents){
					if(err) {
						self.master.emit('error', 'error running the user-provided query', err);
						return redis.del(self.master.id + ':lock', function(err){
							if(err) self.master.emit('error', 'error releasing lock', err);
							return setTimeout(self.consume.bind(self), self.master.options.retryDelay);
						});
					}

					var tasks = contents.map(JSON.stringify);

					var multi = redis.multi();

					// release the lock
					multi = multi.del(self.master.id + ':lock');

					// add tasks to redis queue
					multi = multi.lpush.apply(multi, self.master.id + ':queue', tasks);

					// update the redis cursor
					multi = multi.set(self.master.id + ':cursor', cursor);

					multi.exec(function(err){
						if(err){
							// TODO: this *could* be a bad place to have an error. we need to check which command threw and act accordingly
							self.master.emit('error', 'error populating queue', err);
							return setTimeout(self.consume.bind(self), self.master.options.retryDelay);
						}

						self.master.emit('populate', cursor, tasks);
						return setImmediate(self.consume.bind(self));
					});
				});
			});
		}
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
