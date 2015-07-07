// Encode a message
function _encodeMsg(str) {

    var
        b = new Buffer("XXXXY"+str),
        length = b.length - 5;

    b[0] = (length >> 24  & 0xff);
    b[1] = (length >> 16  & 0xff);
    b[2] = (length >>  8  & 0xff);
    b[3] = (length        & 0xff);
	b[4] = (0x01 << 4) | 0x01;

    return b;

}

process.stdout.write(_encodeMsg("bla bla bla"));
