var
	fs = require('fs'),
	async = require('async'),
    SolidQueue = require('../solidqueue'),
    queue,
	got,
	n = 0,
	took;

// Remove the queue file
try { fs.unlinkSync("test/data/file_async.db"); } catch(ex) { }

// Create the queue
queue = new SolidQueue({file:"test/data/file_async.db"});

queue.on('ready',function(){

	var
		start = new Date();

	// Item processor
	queue.on('nextItem',function(_nItem,when){
		++n;

		// Shift
		queue.shift(function(err,item,ack){
			if ( err ) {
				console.log("nextItem: failed! Error shifting data for test #"+n+": ",err);
				return process.exit(-2);
			}

			setTimeout(function(){
				// Acknowledge
				ack(function(err,ok){
					if ( err ) {
						console.log("nextItem: failed! Error acknowledging item "+item+": ",err);
						return process.exit(-1);
					}

					if ( n == 3 ) {
						if ( item != 125 ) {
							console.log("nextItem: failed! Expecting that item #3 would be 125 and was "+item);
							return process.exit(-1);
						}
						took = new Date()-start;
						if ( took < 3000 ) {
							console.log("nextItem: failed! Got the third item too fast. I was expecting at least 3 seconds for that and it just took "+took+" ms");
							return process.exit(-1);
						}

						console.log("nextItem: ok");
						return process.exit(0);
					}
				});
			},1000);

		});

	});

	queue.push(123,function(){});
	queue.push(124,function(){});
	queue.push(125,function(){});
});

