var
	fs		= require('fs'),
	uuid		= require('uuid'),
	header		= new Buffer("OIIIIIIIIIIIIIIIISSSST"),
	fd,
	file = process.argv[2] || 'queue.db';

// Decode the header of an entry
function _entryDecodeHeader(b) {

	var
		item = {};

	// Operation
	item.op = b[0];

	// Convert the binary ID into a string ID
	try {
		item.id = uuid.unparse(b,1);
	}
	catch(ex){
		console.log("Unable to parse ID");
		item.id = null;
	}

	// Data size
	item.size = (b[17] << 24) | (b[18] << 16) | (b[19] << 8) | b[20];

	// Type
	item.type = b[21];

	return item;

}

// Open and read all data inside of the file
fd = fs.openSync(file,"r");
while ( fs.readSync(fd,header,0,header.length) ) {
	var
		entry = _entryDecodeHeader(header),
		data = new Buffer(entry.size),
		rBytes;

	// Read data
	if ( entry.size > 0 && (rBytes = fs.readSync(fd,data,0,entry.size)) < entry.size ) {
		console.log("Queue file appears to be corrupted (header size is "+entry.size+" and got "+rBytes+")");
		break;
	}

	console.log("OP: ",entry.op);
	console.log("ID: ",entry.id);
	console.log("SI: ",entry.size);
	console.log("TY: ",entry.type);

	// Parse
	if ( entry.size > 0 ) {
		data = (entry.type == 1) ? data.toString() : (entry.type == 2) ? parseFloat(data.toString()) : (entry.type == 3) ? null : JSON.parse(data);
		console.log("DT: ",data," ("+entry.size+" bytes)");
	}
}
fs.close(fd);
process.exit(0);