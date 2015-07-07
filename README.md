# solidqueue: A solid and safe (stateful and crash-safe) queue with a simple interface

`solidqueue` can be used either on syncronous and asyncronous ways. It saves the queue state on a file and supports journaling.

Syncronous:

	var
	    SolidQueue = require('solidqueue'),
	    queue = new SolidQueue({file:file.db",sync:true});

	queue.push(123);
	console.log("Got: ",queue.shift());

Asyncronous:

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
