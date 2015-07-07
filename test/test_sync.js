var
	fs = require('fs'),
    SolidQueue = require('../solidqueue'),
    queue,
	got;

// Remove the queue file
try { fs.unlink("test/data/file_sync.db"); } catch(ex) { }

// Create the queue
queue = new SolidQueue("test/data/file_sync.db");

// Push a number
queue.push("a");
got = queue.shift();
if ( typeof got != "string" || got != "a" ) {
	console.log("shift(push(string)): failed! Expecting a number = \"a\" and got "+((got==null)?"nothing":("a "+typeof(got)+" = "+got)));
	return process.exit(-1);
}
console.log("shift(push(string)): ok");

// Push a number
queue.push(123);
got = queue.shift();
if ( typeof got != "number" || got != 123 ) {
	console.log("shift(push(number)): failed! Expecting a number = 123 and got "+((got==null)?"nothing":("a "+typeof(got)+" = "+got)));
	return process.exit(-1);
}
console.log("shift(push(number)): ok");

// Push an object
queue.push({x:1});
got = queue.shift();
if ( typeof got != "object" || Object.keys(got).length != 1 || got.x != 1 ) {
	console.log("shift(push(number)): failed! Expecting an object = {x:1} and got "+((got==null)?"nothing":("a "+typeof(got)+" = "+JSON.stringify(got))));
	return process.exit(-1);
}
console.log("shift(push(object)): ok");

return process.exit(0);
