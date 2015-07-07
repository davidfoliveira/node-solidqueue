var
	fs		= require('fs'),
	header	= new Buffer(5),
	fd;

// Open and read all data inside of the file
fd = fs.openSync("file_async.db","r");
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

	console.log("OP: ",op);
	console.log("TY: ",type);
	console.log("SI: ",size);

	// Parse
	if ( size > 0 ) {
		data = (type == 1) ? data.toString() : (type == 2) ? parseFloat(data.toString()) : (type == 3) ? null : JSON.parse(data);
		console.log("DT: ",data," ("+size+" bytes)");
	}
}
fs.close(fd);
process.exit(0);
