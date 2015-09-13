var
	fs			= require('fs'),
	async		= require('async'),
    SolidQueue	= require('../solidqueue'),
	n			= 0,
	shifted		= 0,
	got			= -1,
	acked		= 0,
	results 	= [2000,1500,1800,4000,1000,1200],
	times   	= [1500,1800,2000,2500,3000,4000],
	start		= new Date(),
	drained		= false,
    queue;

// Remove the queue file
try { fs.unlinkSync("test/data/file_async.db"); } catch(ex) { }

// Create the queue
queue = new SolidQueue({file:"test/data/file_async.db",concurrentItems: 4});

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
			shifted++;

			setTimeout(function(){
				got++;

//				if ( results[got] != item ) {
//					console.log("Expecting to get result "+item+" and got "+results[got]);
//					return process.exit(-1);
//				}
				if ( new Date() - start < times[got] || new Date() - start > times[got]+100 ) {
					console.log("Expecting to get result "+results[got]+" in "+times[got]+" ms but got in "+(new Date()-start)+" ms");
					return process.exit(-1);
				}

				// Acknowledge
				ack(function(err,ok){
					if ( err ) {
						console.log("nextItem: failed! Error acknowledging item "+item+": ",err);
						return process.exit(-1);
					}
					acked++;
				});
			},item);

		});

	});

	results.forEach(function(i){
		queue.push(i,function(){});
	});
});

queue.on('drain',function(){
	if ( shifted != 6 ) {
		console.log("Expecting to get 'drain' event when the number of shifted items is 6 but got it when is "+shifted);
		return process.exit(-1);
	}
	drained = true;
});
queue.on('free',function(){
	if ( shifted != 6 || got != 5 || acked != 6 ) {
		console.log("Expecting to get 'free' event when the number of shifted/got/acked items is 6/5/6 got is "+shifted+"/"+got+"/"+acked);
		return process.exit(-1);
	}
	console.log("nextItem(4ci): ok");
	return process.exit(0);
});
