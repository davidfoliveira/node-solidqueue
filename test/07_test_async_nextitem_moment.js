var
	fs = require('fs'),
	async = require('async'),
    SolidQueue = require('../solidqueue'),
    queue,
	n = 0,
	acks = 0,
	took;

// Remove the queue file
try { fs.unlinkSync("test/data/file_async.db"); } catch(ex) { }

// Create the queue
queue = new SolidQueue({file:"test/data/file_async.db"});

queue.on('ready',function(){

	var
		start = new Date();

	// Item processor
	queue.on('nextItem',function(){
		n++;
		if ( n != acks + 1 ) {
			console.log("nextItem(moment): failed! Got nextItem #"+n+" before previous acknowledge #"+acks);
			return process.exit(-1);
		}

		// Shift
		queue.shift(function(err,item,ack){
			if ( err ) {
				console.log("nextItem(moment): failed! Error shifting data for test #"+n+": ",err);
				return process.exit(-2);
			}

			// Acknowledge
			ack(function(err,ok){
				if ( err ) {
					console.log("nextItem(moment): failed! Error acknowledging item "+item+": ",err);
					return process.exit(-1);
				}
				acks++;

				if ( item == 125 ) {
					console.log("nextItem(moment): ok");
					return process.exit(0);
				}
			});

		});

	});

	queue.push(123,function(){});
	queue.push(124,function(){});
	queue.push(125,function(){});
});

