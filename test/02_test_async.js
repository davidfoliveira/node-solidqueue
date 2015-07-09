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
		tests = ["a",123,{ok:1}],
		n = 0;

	async.mapSeries(tests,
		function(test,next){
			n++;

			// Push
			queue.push(test,function(err){
				if ( err ) {
					console.log("shift(push("+JSON.stringify(test)+")): failed! Error pushing data for test #"+n+": ",err);
					return process.exit(-2);
				}

				// Shift
				queue.shift(function(err,item,ack){
					if ( err ) {
						console.log("shift(push("+JSON.stringify(test)+")): failed! Error shifting data for test #"+n+": ",err);
						return process.exit(-2);
					}

					if ( JSON.stringify(item) != JSON.stringify(test) ) {
						console.log("shift(push("+JSON.stringify(test)+")): failed! Expecting "+JSON.stringify(test)+" and got "+JSON.stringify(item));
						return process.exit(-1);
					}

					// Acknowledge
					ack(function(err,ok){
						if ( err ) {
							console.log("shift(push("+JSON.stringify(test)+")): failed! Error acknowledging item "+item+": ",err);
							return process.exit(-1);
						}

						console.log("shift(push("+JSON.stringify(test)+")): ok");
						return next();
					});
				});
			});
		},
		function(err,res){

			// Compile and watch the file size, as do be zero
			queue.compile(function(err){
				if ( err ) {
					console.log("fsize(): failed! Error compiling queue file: ",err);
					return process.exit(-2);
				}

				stat = fs.statSync("test/data/file_async.db");
				if ( stat.size != 0 ) {
					console.log("fsize(): failed. Expecting a queue file with zero bytes but the file has "+stat.size+" bytes");
					return process.exit(-1);
				}
				console.log("fsize(): ok");

				return process.exit(0);
			});
		}
	);
});

