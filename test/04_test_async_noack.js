var
	fs = require('fs'),
	async = require('async'),
    SolidQueue = require('../solidqueue'),
    queue;


// Remove the queue file
try { fs.unlinkSync("test/data/file_async.db"); } catch(ex) { }

// Create the queue
queue = new SolidQueue({file:"test/data/file_async.db"});

queue.on('ready',function(){

	// Shift
	queue.shift(function(err,item,ack){
		if ( err ) {
			console.log("shift(NOACK): failed! Error shifting data for test #"+n+": ",err);
			return process.exit(-2);
		}

		if ( item != 201 ) {
			console.log("shift(NOACK): failed. Expected to get 102 as values but got "+item);
			return process.exit(-1);
		}

		// Compile
		queue.compile(function(err){
			if ( err ) {
				console.log("shift(NOACK): failed! Error compiling the queue file: ",err);
				return process.exit(-2);
			}

			stat = fs.statSync("test/data/file_async.db");
			if ( stat.size == 0 ) {
				console.log("shift(NOACK): failed. Expecting a queue file with more than zero bytes but the file has "+stat.size+" bytes");
				return process.exit(-1);
			}

			console.log("shift(NOACK): ok");
			return process.exit(0);
		});
	});

	queue.push(201,function(err){
		if ( err ) {
			console.log("shift(NOACK): failed! Error pushing data (first) into the queue");
			return process.exit(-2);
		}
	});
});

