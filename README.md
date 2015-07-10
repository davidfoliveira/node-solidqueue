# solidqueue: A solid and safe (stateful and crash-safe) queue with a simple interface

`solidqueue` can be used either on synchronous and asynchronous ways. It saves the queue state on a file and supports journaling.

## Synchronous:

	var
	    SolidQueue = require('solidqueue'),
	    queue = new SolidQueue({file:file.db",sync:true}),
		rv;

	queue.push(123);
	rv = queue.shift();
	console.log("Got: ",rv.data);
	rv.ack();

### Available methods (synchronous mode):

* `push(value)`: Pushes an item into the queue. Throws an error is something unexpected happens.
* `shift()`: Picks an item from the queue and returns an object like `{data:XX,ack:fn()}` where `data` is the previously pushed item and `ack` is the acknowledge function. Throws an error is something unexpected happens.

### Available events (synchronous mode):

* `ready`: Fired when the queue is ready.
* `error`: Fired when some error happens. The event handler is called with two arguments, the phase on what the error happened and the error itself.


## Asynchronous:

	var
	    SolidQueue = require('solidqueue'),
	    queue = new SolidQueue({file:"file.db"});

	queue.on('ready',function(){
	    return queue.push({bla:"bli"},function(err){
	        if ( err ) {
	            console.log("Error pushing item to queue: ",err);
	            return;
	        }

	        return queue.shift(function(err,item){
	            if ( err ) {
	                console.log("Error shifting item from queue: ",err);
	                return;
	            }
	            console.log("Got: ",item);
                return process.exit(0);
	        });
	    });
	});

### Available methods (asynchronous mode):

* `push(value,callback)`: Pushes an item into the queue and calls the callback. The arguments of callback function are: `(err)`.

* `shift(callback)`: Picks an item from the queue and calls the callback. The arguments of the callback function are `(err,value,ack)` where `item` is the value previously pushed into the queue and `ack` is the acknowledge function that should be called after processing this queue item.

### Available events (asynchronous mode):

* `ready`: Fired when the queue is ready.
* `error`: Fired when some error happens. The event handler is called with two arguments, the phase on what the error happened and the error itself.
* `nextItem`: Called when a new/next item is available for being processed, which is: after initialization if the queue is not empty, after an acknowledge if the queue is not empty and after a push() if the queue was empty.


