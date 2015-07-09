var
	fs = require('fs'),
	async = require('async'),
    SolidQueue = require('../solidqueue'),
    queue,
	n = 1;

// Remove the queue file
try { fs.unlinkSync("test/data/file_async.db"); } catch(ex) { }

// Create the queue
queue = new SolidQueue({file:"test/data/file_async.db"});

queue.on('ready',function(){

	var
		sstart = new Date();

	// Shift
	queue.shift(function(err,item,ack){
		n++;

		// Acknowledge
		return ack(function(err){
			if ( err ) {
				console.log("shift(WAIT): failed! Error acknowledging the first item: ",err);
				return process.exit(-2);
			}
			// ok
		});
	});
	queue.shift(function(err,item,ack){
		if ( err ) {
			console.log("shift(WAIT): failed! Error shifting data for test #"+n+": ",err);
			return process.exit(-2);
		}

		if ( n < 2 ) {
			console.log("shift(WAIT): failed! Expected to be the second callback but I was the callback #"+n);
			return process.exit(-1);
		}
		if ( item != 102 ) {
			console.log("shift(WAIT): failed! Expected to get 102 as values but got "+item);
			return process.exit(-1);
		}
		if ( new Date()-sstart < 700 ) {
			console.log("shift(WAIT): failed! Got data before the time");
			return process.exit(-1);
		}

		// Acknowledge
		return ack(function(err){
			if ( err ) {
				console.log("shift(WAIT): failed! Error acknowledging the second item: ",err);
				return process.exit(-2);
			}

			console.log("shift(WAIT): ok");
			return process.exit(0);
		});
	});

	setTimeout(function(){
		queue.push(101,function(err){
			if ( err ) {
				console.log("shift(WAIT): failed! Error pushing data (first) into the queue");
				return process.exit(-2);
			}
		});
		queue.push(102,function(err){
			if ( err ) {
				console.log("shift(WAIT): failed! Error pushing data (second) into the queue");
				return process.exit(-2);
			}
		});
	},700);

	setTimeout(function(){
		console.log("shift(WAIT): failed! Passed 2 seconds and got no data");
		return process.exit(-1);
	}, 2000);
});

