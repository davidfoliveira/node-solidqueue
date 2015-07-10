var
	fs = require('fs'),
	async = require('async'),
    SolidQueue = require('../solidqueue'),
    queue,
	got,
	stat;

// Remove the queue file
try { fs.unlinkSync("test/data/file_async.db"); } catch(ex) { }

// Create the queue
queue = new SolidQueue({file:"test/data/file_async.db"});

queue.on('ready',function(){

	// Things to test
	var
		tests	= [],
		ok		= 0,
		got		= 0;

	for ( var n = 0 ; n < 5000 ; n++ )
		tests.push(n);

	// Wait for everybody
	tests.forEach(function(t){
		queue.shift(function(err,v){
			if ( v != t ) {
				console.log("Expecting "+t+" and got "+v);
			}
			else {
				ok++;
			}
			if ( ++got == tests.length ) {
				console.log("shift(5k): ok");
				return process.exit(0);
			}
		});
	});

//	console.log("waiting: ",queue._waitData.length);
	tests.forEach(function(t){
		queue.push(t,function(){});
	});

});

