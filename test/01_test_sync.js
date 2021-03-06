var
	fs = require('fs'),
    SolidQueue = require('../solidqueue'),
    queue,
	item,
	got,
	stat;

// Remove the queue file
try { fs.unlinkSync("test/data/file_sync.db"); } catch(ex) { }

// Create the queue
queue = new SolidQueue({file:"test/data/file_sync.db",sync:true});

// Push a number
queue.push("a");
item = queue.shift();
got = item.data;
if ( typeof got != "string" || got != "a" ) {
	console.log("shift(push(string)): failed! Expecting a string = \"a\" and got "+((got==null)?"nothing":("a "+typeof(got)+" = "+JSON.stringify(got))));
	return process.exit(-1);
}
item.ack();
console.log("shift(push(string)): ok");

// Push a number
queue.push(123);
item = queue.shift();
got = item.data;
if ( typeof got != "number" || got != 123 ) {
	console.log("shift(push(number)): failed! Expecting a number = 123 and got "+((got==null)?"nothing":("a "+typeof(got)+" = "+JSON.stringify(got))));
	return process.exit(-1);
}
item.ack();
console.log("shift(push(number)): ok");

// Push an object
queue.push({x:1});
item = queue.shift();
got = item.data;
if ( typeof got != "object" || Object.keys(got).length != 1 || got.x != 1 ) {
	console.log("shift(push(number)): failed! Expecting an object = {x:1} and got "+((got==null)?"nothing":("a "+typeof(got)+" = "+JSON.stringify(got))));
	return process.exit(-1);
}
item.ack();
console.log("shift(push(object)): ok");

// Compile and watch the file size, as do be zero
queue.compile();

stat = fs.statSync("test/data/file_sync.db");
if ( stat.size != 0 ) {
	console.log("fsize(): failed. Expecting a queue file with zero bytes but the file has "+stat.size+" bytes");
	return process.exit(-1);
}
console.log("fsize(): ok");

return process.exit(0);
