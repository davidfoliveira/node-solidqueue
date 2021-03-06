"use strict";

var
	fs		= require('fs'),
	events	= require('events'),
	util	= require('util'),
	async	= require('async'),
	fnlock	= require('fnlock'),
	uuid	= require('uuid');


// The queue constructor
module.exports = function(opts){

	var
		self = this;

	// My properties
	self._q					= [];
	self._waitAck			= {};
	self._waitAckCount		= 0;
	self._dirty				= false;
	self._compiling			= false;
	self._waitData			= [];
	self._ready				= false;
	self._drain				= true;

	// My methods
	self._loadSync			= _loadSync;
	self._loadAsync			= _loadAsync;
	self._loadProcessEntry	= _loadProcessEntry;
	self._compileFile		= _compileFile;
	self._init				= _init;
	self._initSync			= _initSync;
	self._initAsync			= _initAsync;
	self._itemAckSync		= _itemAckSync;
	self._itemAckAsync		= _itemAckAsync;

	self.push				= queuePush;
	self.shift				= queueShift;
	self.compile			= _compileFile;
	self.store				= _compileFile;
	self.toArray			= function(){ return self._q; };
	self.length				= function(){ return self._q.length; };
	self.size				= self.length;

	// Work on the options
	if ( typeof opts == "string" )
		opts = { file: opts };
	self._opts = opts;

	// Some default settings
	if ( !opts.concurrentItems || typeof opts.concurrentItems != "number" )
		opts.concurrentItems = 1;

	// Initialize
	self._init();

	// Compile the file periodically
	if ( !opts.compileInterval || typeof opts.compileInterval != "number" )
		opts.compileInterval = 2000;

	self._compileInt = setInterval(function(){
		if ( self._ready && !self._compiling && self._dirty )
			self._compileFile();
	},opts.compileInterval);

};
util.inherits(module.exports, events.EventEmitter);


// Initialize (load queue file, open it for appending, compile it and ready!)
function _init(handler) {

	return this._opts.sync ? _initSync.apply(this) : _initAsync.apply(this,[handler]);

}

function _initSync() {

	var	
		self = this;

	// Load the queue file
	self._loadSync();

	// Compile the file
	self._compileFile();

	// Ready!
	self._ready = true;
	self.emit('ready',true);

}

function _initAsync() {

	var
		self = this;

	// Load the queue file
	return self._loadAsync(function(){

		// Compile the file
		return self._compileFile(function(err){
			if ( err ) {
				console.log("Error compiling queue file: ",err);
				self.emit('error','compile',err);
			}

			// Ready!
			self._ready = true;
			self.emit('ready',true);

			// Queue has items? Pick the first and emit 'nextItem'
			if ( self._q.length > 0 )
				self.emit('nextItem',self._q[0],'init');
		});

	});

}

// Load the queue file (synchronously)
function _loadSync() {

	var
		self	= this,
		header	= new Buffer("OIIIIIIIIIIIIIIIISSSST"),
		stats	= { added: 0, removed: 0 },
		fd;

	if ( !fs.existsSync(self._opts.file) )
		return;

	// Open and read all data inside of the file
	fd = fs.openSync(this._opts.file,"r");
	while ( fs.readSync(fd,header,0,header.length) ) {
		var
			entry = _entryDecodeHeader(header),
			rBytes;

		// Read data
		entry.data = new Buffer(entry.size);
		if ( entry.size > 0 && (rBytes = fs.readSync(fd,data,0,entry.size)) < entry.size ) {
			console.log("Queue file appears to be corrupted (header size is "+entry.size+" and got "+rBytes+")");
			break;
		}

		// Process the operation
		self._loadProcessEntry(entry,stats);
	}
	fs.close(fd);

//	console.log("Added: ",stats.added);
//	console.log("Removed: ",stats.removed);
//	console.log("Queue after load: ",self._q.length);

}

// Load the queue file (asynchronous)
function _loadAsync(handler) {

	var
		self	= this,
		header	= new Buffer(5),
		stats	= { added: 0, removed: 0 },
		fd;

	// Check if the file exists
	return fs.exists(self._opts.file,function(exists){
		if ( !exists )
			return handler(null,false);

		// Open file for reading
		return fs.open(self._opts.file,"r",function(err,fd){
			if ( err ) {
				console.log("Error openning queue file '"+opts.file+"': ",err);
				self.emit('error','load',err);
				return;
			}

			var
				hasData = true;

			// Read the file in parts
			return async.whilst(
				function(){return hasData;},
				function(next){
					var
						header = new Buffer("OIIIIIIIIIIIIIIIISSSST");

					// Read the header
					return fs.read(fd,header,0,header.length,null,function(err,rBytes){
						if ( err ) {
							console.log("Error reading from queue file '"+opts.file+"': ",err);
							hasData = false;
							return next(err,null);
						}

						// No data?
						if ( rBytes == 0 ) {
							hasData = false;
							return next();
						}
						if ( rBytes < header.length ) {
							console.log("Error reading data entry header from queue file (got "+rBytes+" instead of "+header.length+")");
							hasData = false;
							return next(err,null);
						}

						var
							entry	= _entryDecodeHeader(header);

						// If this item has data, read it!
						return aIf(entry.size > 0,
							function(proc){

								// Read the data
								entry.data = new Buffer(entry.size);
								return fs.read(fd,entry.data,0,entry.size,null,function(err,rBytes){
									if ( rBytes < entry.size ) {
										console.log("Error reading data entry from queue file (got "+rBytes+" instead of "+size+")");
										hasData = false;
										return next(err,null);
									}

									return proc();
								});
							},
							function(){

								// Process the operation
								self._loadProcessEntry(entry,stats);

								return next();
							}
						);

					});

				},
				function(err){
					if ( err ) {
						console.log("Error reading queue file: ",err);
						self.emit('error','load',err);
						return handler(err,null);
					}

//					console.log("Added: ",stats.added);
//					console.log("Removed: ",stats.removed);
//					console.log("Queue after load: ",self._q.length);
					return handler(null,stats);
				}
			);
		});

	});

}

// Process an operation during load
function _loadProcessEntry(entry,stats) {

	var
		self	= this,
		found	= null;

	// Add to queue
	if ( entry.op == 1 ) {
		// Parse data according to it's type
		entry.data = (entry.type == 1) ? entry.data.toString() : (entry.type == 2) ? parseFloat(entry.data.toString()) : (entry.type == 3) ? null : JSON.parse(entry.data);

		// Add to queue
		this._q.push(entry);

		// Affect statistics
		if ( stats )
			stats.added++;
	}
	// Remove from queue
	else if ( entry.op == 2 ) {
		// Search the item on the queue
		for ( var x = 0 ; x < self._q.length ; x++ ) {
			if ( self._q[x].id == entry.id ) {
				found = x;
				break;
			}
		}
		if ( found == null ) {
			console.log("Weird stuff. Found on the queue file the ack of an item that is not on the queue.. well, ignoring it..");
			return;
		}

		// Remove from the queue
		self._q.splice(found,1);

		// Affect statistics
		if ( stats )
			stats.removed++;
	}

}


// (re)Compile the file
function _compileFile(handler) {

	var
		self = this;

//	console.log("Compiling...");
	return self._opts.sync ? _compileFileSync.apply(self) : _compileFileAsync.apply(self,[handler]);

}

function _compileFileSync() {

	var
		self = this,
		fd,
		toCompile;

	self._compiling = true;

	// Close the already open file
	if ( self._fd != null ) {
		fs.closeSync(self._fd);
		self._fd = null;
	}

	// Open file again for rewriting it
	self._fd = fs.openSync(self._opts.file,"w");
	toCompile = self._q.slice(0);
	for ( var k in self._waitAck ) {
		if ( self._waitAck[k] )
			toCompile.push(self._waitAck[k]);
	}
	toCompile.forEach(function(item){
		var
			b = _itemToBuffer(item);

		fs.writeSync(this._fd,b,0,b.length,null);
	});

	// Sync it
	fs.fsyncSync(this._fd);

	// Done
	self._compiling = false;

}

function _compileFileAsync(handler) {

	var
		self = this,
		tempFD,
		toCompile;


	// Mark as compiling...
	self._compiling = true;

	// Lock the file writing
	return fnlock.lock('fileWrite',function(release){

		// All process of compile
		return async.series(
			[
				// Open temporary file for writing
				function(next){
					return fs.open(self._opts.file+".tmp","w",function(err,fd){
						if ( err )
							console.log("Error openning temporary queue file '"+self._opts.file+"' for writing: ",err);
						else
							tempFD = fd;

						return next(err,fd);
					});
				},

				// Write all the items on the queue and waiting acknowledge
				function(next){

					// Build the list of items to be compiled
					toCompile = self._q.slice(0);
					for ( var k in self._waitAck ) {
						if ( self._waitAck[k] )
							toCompile.push(self._waitAck[k]);
					}

					// Write all the data
					return async.mapSeries(toCompile,
						function(item,nextItem){
							var
								b = _itemToBuffer(item);

							// Write
							return fs.write(tempFD,b,0,b.length,null,nextItem);
						},
						function(err,res){
							if ( err )
								console.log("Error writing data to temporary queue file: ",err);

							return next(err,res);
						}
					);
				},

				// Close the file
				function(next){
					return fs.close(tempFD,function(err){
						if ( err )
							console.log("Error closing temporary queue file '"+self._opts.file+"': ",err);

						return next(err,null);
					});
				},

				// Move the temporary file to definitive
				function(next) {
					return fs.rename(self._opts.file+".tmp",self._opts.file,function(err){
						if ( err )
							console.log("Error moving temporary file '"+self._opts.file+"'.tmp to definitive "+self._opts.file);

						return next(err,null);
					});
				},

				// Close the description (if it's open)
				function(next){
					if ( self._fd == null )
						return next(null,false);

					// Close the file
					return fs.close(self._fd,function(err){
						if ( err )
							console.log("Error closing queue file '"+self._opts.file+"' for reopening: ",err);

						return next(err,null);
					});
				},

				// Open the queue file
				function(next){
					return fs.open(self._opts.file,"a",function(err,fd){
						if ( err )
							console.log("Error openning queue file '"+opts.file+"': ",err);
						else
							self._fd = fd;

						return next(err,fd);
					});
				}
			],
			function(err,res){
				if ( err ) {
					console.log("Error compiling queue file '"+self._opts.file+"': ",err);
					self.emit('error','compile',err);
				}
				else {
//					console.log("Successfully compiled queue file");
					self.emit('compile',true);
				}

				self._dirty = false;
				self._compiling = false;
				release();
				return handler ? handler(err,res) : null;
			}
		);

	});

}


// Push something into to the queue
function queuePush(data,handler) {

	var
		self = this,
		item,
		b,
		startLen = self._q.length,
		checkWaitersAndGo = function(err,res){
			// Call the callback
			handler(err,res);

			// Distribute queue items by waiting handlers
			if ( self._waitData.length > 0 ) {
				while ( self._waitData.length > 0 && self._q.length > 0 ) {
					_queueShift.apply(self,[self._waitData.shift()]);
				}
			}
//			else if ( startLen == 0 && self._waitAckCount == 0 ) {
			else {
//				console.log("CI: ",self._opts.concurrentItems);
//				console.log("QL: ",startLen);
//				console.log("WA: ",self._waitAckCount);
				if ( startLen < self._opts.concurrentItems && self._opts.concurrentItems > self._waitAckCount ) {
//console.log("CALLING NEXTITEM");
					// Emit nextItem
					self.emit('nextItem',self._q[0],'push');
				}
				else {
//					console.log("NO CALL");
				}
			}
		};

//	if ( !handler && !this._opts.sync )
//		throw new Error("Trying to use a synchronous version of push() but the queue is not on synchronous mode (sync option)");

	if ( !this._ready )
		throw new Error("The queue is not ready yet. Wait for 'ready' event");

	// Create the item
	item = {
		op:		1,
		id:		uuid.v1(),
		data:	data
	};

	// Add to memory queue
	this._q.push(item);

	// We are dirty (requiring a compile)
	this._dirty = true;

	// Write on the file
	b = _itemToBuffer(item);

	// Add to file
	return handler ? _writeFileAsync.apply(this,[b,checkWaitersAndGo]) : _writeFileSync.apply(this,[b]);

}

// Get something from the queue
function queueShift(handler) {

	var
		self = this,
		item,
		b;

	if ( !handler && !this._opts.sync )
		throw new Error("Trying to use a synchronous version of push() but the queue is not on synchronous mode (sync option)");

	if ( !this._ready )
		throw new Error("The queue is not yet ready. Wait for 'ready' event");

	// Nothing in memory, nothing on the file
	if ( self._q.length == 0 ) {
		// Synchronous mode just returns null (what can we do?)
		if ( self._opts.sync )
			return null;

		// Asynchronous mode registers the handler that will be called when we have data
		return self._waitData.push(handler);
	}

	// Now that we have data, proceed!
	return _queueShift.apply(self,[handler]);

}

// Really shift, not kidding
function _queueShift(handler) {

	var
		self = this,
		item;

	// Get data from memory
	item = self._q.shift();

	// This item will be waiting for acknowledge
	self._waitAck[item.id] = item;
	self._waitAckCount++;

	// Return the data and the acknowledge function
	if ( self._opts.sync ) {
		return {data: item.data, ack: function(){
			return self._itemAckSync(item);
		}};
	}
	else {
		handler(null,item.data,function(cb){
			return self._itemAckAsync(item,cb);
		},item);
		if ( self._q.length == 0 )
			self.emit('drain');
	}
}

// Acknowledge an item (synchronous)
function _itemAckSync(item) {

	var
		b;

	// Delete from acknowledge waiting list
//	delete this._waitAck[item.id];
	this._waitAck[item.id] = null;
	this._waitAckCount--;

	// Write a "shift" to file
	b = _entryEncode({op:2,id:item.id});

	// Remove from file asynchronously
	_writeFileSync.apply(this,[b]);

	// We are dirty (requiring a compile)
	this._dirty = true;

//	console.log("Sync ack of '"+item.id+"'");

}

// Acknowledge an item (asynchronous)
function _itemAckAsync(item,handler) {

	var
		self = this,
		b;

	// Write a "shift" to file
	b = _entryEncode({op:2,id:item.id});
	return _writeFileAsync.apply(self,[b,function(err){
		if ( err ) {
			console.log("Error writing the shift to disk: ",err);
			return handler(err,null);
		}

		// We are dirty (requiring a compile)
		self._dirty = true;

		// Delete from acknowledge waiting list
//		delete this._waitAck[item.id];
		self._waitAck[item.id] = null;
		self._waitAckCount--;


		// Tell that we acknowledged
		if ( handler )
			handler(null,true);

		// Pick the next item on the queue
		if ( self._q.length > 0 )
			self.emit('nextItem',self._q[0],'ack');
		else if ( self._waitAckCount == 0 )
			self.emit('free');

//		console.log("Async ack of '"+item.id+"'");
	}]);

}


// Convert a data item into a buffer for being stored
function _itemToBuffer(item) {

	var
		strData;

	// Detect item type
	item.type = (typeof item.data == "string") ? 1 : (typeof item.data == "number") ? 2 : (item.data == null) ? 3 : 4;

	// Convert item into a string
	strData = (item.type == 1) ? item.data : (item.type == 2) ? item.data.toString() : (item.type == 3) ? "" : JSON.stringify(item.data);

	// Encode it
    return _entryEncode(item,strData);

}

// Encode an entry
function _entryEncode(item,strData) {

	var
		b = new Buffer("OIIIIIIIIIIIIIIIISSSST"+((strData == null)?"":strData)), // O=OP, I=ID, S=SIZE, T=TYPE
		size;

	// Operation
	b[0] = item.op;

	// Convert the ID into a binary and store it on buffer
	uuid.parse(item.id,b,1);

	// Data size
	size = b.length - 22;
	b[17] = (size >> 24  & 0xff);
	b[18] = (size >> 16  & 0xff);
	b[19] = (size >>  8  & 0xff);
	b[20] = (size        & 0xff);

	// Set the type
	b[21] = item.type;

	return b;

}

// Decode the header of an entry
function _entryDecodeHeader(b) {

	var
		item = {};

	// Operation
	item.op = b[0];

	// Convert the binary ID into a string ID
	item.id = uuid.unparse(b,1);

	// Data size
	item.size = (b[17] << 24) | (b[18] << 16) | (b[19] << 8) | b[20];

	// Type
	item.type = b[21];

	return item;

}


// Write to the file, synchronous or asynchronous
function _writeFileSync(data) {

	fs.writeSync(this._fd,data,0,data.length,null);
	fs.fsyncSync(this._fd);

}

function _writeFileAsync(data,handler) {

	var
		self = this;

	// Lock the file writing (we can't write at the same time)
	fnlock.lock('fileWrite',function(release){

		return fs.write(self._fd,data,0,data.length,null,function(err,res){
			if ( err ) {
				console.log("Error writing data to file: ",err);
				return handler(err,null);
			}

			// Sync
			return fs.fsync(self._fd,function(err){
				if ( err ) {
					console.log("Error sync'ing data to disk: ",err);
					return handler(err,null);
				}

				// OK
				release();
				return handler(null,res);
			});
		});
	});
}

// Utils
function aIf(cond,a,b){
	return cond ? a(b) : b();
}
