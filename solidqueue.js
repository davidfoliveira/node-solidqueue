"use strict";

var
	fs		= require('fs'),
	events	= require('events'),
	util	= require('util'),
	async	= require('async'),
	fnlock	= require('fnlock');


// The queue
module.exports = function(opts){

	var
		self = this;

	// My properties
	self._q = [];
	self._dirty = false;
	self._compiling = false;

	// My methods
	self._loadSync		= _loadSync;
	self._loadAsync		= _loadAsync;
	self._loadProcessOp = _loadProcessOp;
	self._compileFile	= _compileFile;
	self._waitCompile	= [];
	self._ready			= false;
	self.push			= queuePush;
	self.shift			= queueShift;
	self.compile		= _compileFile;
	self.toArray		= function(){ return self._q; };

	// Work on the options
	if ( typeof opts == "string" )
		opts = { file: opts, sync: true };
	self._opts = opts;

	// Syncronous way of initializing
	if ( opts.sync ) {
		// Load the queue file
		self._loadSync();

		// Open file for writing (append, like a journal)
		self._fd = fs.openSync(self._opts.file,"a");

		// Compile the file
		self._compileFile();

		// Ready!
		self.emit('ready',true);
	}

	// Asyncronous way of initializing
	else {
		self._loadAsync(function(){

			// Open file for writing (append, like a journal)
			return fs.open(self._opts.file,"a",function(err,fd){
				if ( err ) {
					console.log("Error openning queue file '"+self._opts.file+"' for writing: ",err);
					self.emit('error','open',err);
				}
				else {
					self._fd = fd;
				}

				// Compile the file
				return self._compileFile(function(err){
					if ( err ) {
						console.log("Error compiling queue file: ",err);
						self.emit('error','compile',err);
					}

					// Ready!
					self._ready = true;
					self.emit('ready',true);
				});
			});

		});
	}


	// Compile the file periodically
	if ( !opts.compileInterval || typeof opts.compileInterval != "number" )
		opts.compileInterval = 2000;

	self._compileInt = setInterval(function(){
		if ( self._ready && !self._compiling && self._dirty )
			self._compileFile();
	},opts.compileInterval);

};
util.inherits(module.exports, events.EventEmitter);


// Load the queue file (syncronously)
function _loadSync() {

	var
		self	= this,
		header	= new Buffer(5),
		stats	= { added: 0, removed: 0 },
		fd;

	if ( !fs.existsSync(self._opts.file) )
		return;

	// Open and read all data inside of the file
	fd = fs.openSync(this._opts.file,"r");
	while ( fs.readSync(fd,header,0,5) ) {
		var
			op   = (header[0] >> 4) & 0x0f,
			type = header[0] & 0x0f,
			size = (header[1] << 24) | (header[2] << 16) | (header[3] << 8) | header[4],
			data = new Buffer(size),
			rBytes;

		// Read data
		if ( size > 0 && (rBytes = fs.readSync(fd,data,0,size)) < size ) {
			console.log("Queue file appears to be corrupted (header size is "+size+" and got "+rBytes+")");
			break;
		}

		// Process the operation
		self._loadProcessOp(op,type,data,stats);
	}
	fs.close(fd);

//	console.log("Added: ",stats.added);
//	console.log("Removed: ",stats.removed);
//	console.log("Queue after load: ",self._q.length);

}

// Load the queue file (asyncronously)
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
				self.emit('ready');
				return;
			}

			var
				hasData = true;

			// Read the file in parts
			async.whilst(
				function(){return hasData;},
				function(next){
					var
						header = new Buffer("YXXXX");

					// Read the header
					return fs.read(fd,header,0,5,null,function(err,rBytes){
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
						if ( rBytes < 5 ) {
							console.log("Error reading data entry header from queue file (got "+rBytes+" instead of 5)");
							hasData = false;
							return next(err,null);
						}

						var
							op   = (header[0] >> 4) & 0x0f,
							type = header[0] & 0x0f,
							size = (header[1] << 24) | (header[2] << 16) | (header[3] << 8) | header[4],
							data = new Buffer(size);

						aIf(size > 0,
							function(proc){

								// Read the data
								return fs.read(fd,data,0,size,null,function(err,rBytes){
									if ( rBytes < size ) {
										console.log("Error reading data entry from queue file (got "+rBytes+" instead of "+size+")");
										hasData = false;
										return next(err,null);
									}

									return proc();
								});
							},
							function(){
								// Process the operation
								self._loadProcessOp(op,type,data,stats);

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

function _loadProcessOp(op,type,data,stats) {

		// Add to queue
		if ( op == 1 ) {
			// Parse data according to it's type
			data = (type == 1) ? data.toString() : (type == 2) ? parseFloat(data.toString()) : (type == 3) ? null : JSON.parse(data);

			// Add to queue
			this._q.push({type: type, data: data});

			// Affect statistics
			if ( stats )
				stats.added++;
		}
		// Remove from queue
		else if ( op == 2 ) {

			// Remove from queue
			this._q.shift();

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
		fd;

	self._compiling = true;

	// Close the already open file
	fs.closeSync(self._fd);
	self._fd = null;

	// Open file again for rewriting it
	self._fd = fs.openSync(self._opts.file,"w");
	self._q.forEach(function(item){
		var strData;
		strData = (type == 1) ? data : (type == 2) ? data.toString() : (type == 3) ? "" : JSON.stringify(data);
	  	b = new Buffer("YXXXX"+strData);
	    size = b.length - 5;
	    b[0] = (0x01 << 4) | type;
	    b[1] = (size >> 24  & 0xff);
	    b[2] = (size >> 16  & 0xff);
	    b[3] = (size >>  8  & 0xff);
	    b[4] = (size        & 0xff);
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
		finish = function(err,status){
			if ( err ) {
				// FIX ME
				console.log("Error compiling file. Have no big idea about what to do...");
			}
//			else
//				console.log("Compiled!");
			self._compiling = false;
			return handler ? handler(err,status) : null;
		};

	self._compiling = true;

	// Lock the file writing
	fnlock.lock('fileWrite',function(release){

		// Close the file
		fs.close(self._fd,function(err){
			if ( err ) {
				console.log("Error closing queue file '"+self._opts.file+"': ",err);
				release();
				return finish(err,null);
			}
			self._fd = null;

			// Open file for rewriting
			return fs.open(self._opts.file,"w",function(err,fd){
				if ( err ) {
					console.log("Error openning queue file '"+self._opts.file+"' for writing: ",err);
					release();
					return finish(err,null);
				}
				self._fd = fd;

				// Write all the data
				return async.mapSeries(self._q,
					function(item,next){
						var strData;
						strData = (type == 1) ? item : (type == 2) ? item.toString() : (type == 3) ? "" : JSON.stringify(item);
					  	b = new Buffer("YXXXX"+strData);
					    size = b.length - 5;
					    b[0] = (0x01 << 4) | type;
					    b[1] = (size >> 24  & 0xff);
					    b[2] = (size >> 16  & 0xff);
					    b[3] = (size >>  8  & 0xff);
					    b[4] = (size        & 0xff);

					    // Write
					    return fs.write(self._fd,b,0,b.length,null,function(err,res){
					    	if ( err )
					    		return next(err,null);

					    	// Next!
					    	return next();
					    });
					},
					function(err,res){
						if ( err ) {
					    	console.log("Error writing data to queue file: ",err);
					    	release();
					    	return finish(err,null);
					    }

					    // Sync data
					    return fs.fsync(self._fd,function(err){
					    	if ( err ) {
					    		console.log("Error syncing queue file: ",err);
					    		release();
					    		return finish(err,null);
					    	}

							release();
					    	return finish(null,true);
					    });
					}
				);
			});
		});
	});

}



// Push something into to the queue
function queuePush(data,handler) {

	var
		strData,
		type = (typeof data == "string") ? 1 : (typeof data == "number") ? 2 : (data == null) ? 3 : 4,
		b,
		size;

    if ( !handler && !this._opts.sync )
    	throw new Error("Trying to use a syncronous version of push() but the queue is not on syncronous mode (sync option)");

	// Add to memory queue
	this._q.push({type: type, data: data});
	this._dirty = true;

	// Write on the file
	strData = (type == 1) ? data : (type == 2) ? data.toString() : (type == 3) ? "" : JSON.stringify(data);
  	b = new Buffer("YXXXX"+strData);
    size = b.length - 5;
    b[0] = (0x01 << 4) | type;
    b[1] = (size >> 24  & 0xff);
    b[2] = (size >> 16  & 0xff);
    b[3] = (size >>  8  & 0xff);
    b[4] = (size        & 0xff);

	// Add to file
	return handler ? _writeFileAsync.apply(this,[b,handler]) : _writeFileSync.apply(this,[b]);

}

// Get something from the queue
function queueShift(handler) {

	var
		item,
		b,
		size;

    if ( !handler && !this._opts.sync )
    	throw new Error("Trying to use a syncronous version of push() but the queue is not on syncronous mode (sync option)");

	// Nothing in memory, nothing on the file
	if ( this._q.length == 0 )
		return handler ? handler(null,null) : null;

	// Get data from memory
	item = this._q.shift();
	this._dirty = true;

	// Write a "shift" to file
  	b = new Buffer("YXXXX");
    size = b.length - 5;
    b[0] = (2 << 4);
	b[1] = b[2] = b[3] = b[4] = 0;

	// Remove from file asyncronously
	if ( !this._opts.sync ) {
		return _writeFileAsync.apply(this,[b,function(err){
			if ( err ) {
				console.log("Error writing the shift to disk: ",err);
				return handler(err,null);
			}

			// Return the data
			return handler(null,item.data);
		}]);
	}

	// Remove from file syncronously
	_writeFileSync.apply(this,[b]);

	// Return the data
	return item.data;

}

// Write to the file, syncronously or asyncronously
function _writeFileSync(data) {

	fs.writeSync(this._fd,data,0,data.length,null);
	fs.fsyncSync(this._fd);

}

function _writeFileAsync(data,handler) {

	var
		self = this;

	// Lock the file writing
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
