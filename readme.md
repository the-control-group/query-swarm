Query Swarm
===========

Query Swarm allows you to safely distribute query-driven tasks over a swarm of parallel functions on a single process or across multiple processes/machines.

####Use when the following are important:
- Tasks should be allowed a long time to complete
- Each task must only ever be processed once
- Failed tasks must not just "disappear"
- Nodes should shutdown gracefully

####Do *not* use if the following are required:
- Tasks must be strictly processed in order

Requirements
------------

Query Swarm relies on [redis](redis.io) to store the lock, cursor, queue, and lists responsible for coordination. For testing, you can use [redis-mock](https://github.com/faeldt/redis-mock) or any other library that conforms to the [node_redis](https://github.com/mranney/node_redis) interface.


Usage
-----

TODO


Events
------

- error: Error, [message]
- populate: cursor, tasks
- consume: task, [result]
- deadletter: task
- acknowledge: task
