var compress_buffer = require("./index.js");
var crc_utils = require("crc-utils2");

var a = compress_buffer.compress(new Buffer("a"), -1);
var b = compress_buffer.compress(new Buffer("b"), -1);
var ab = compress_buffer.compress(new Buffer("ab"), -1);

var crc = crc_utils.crc32_combine_multi([a,b]);

console.log("BUFOR A", a);
console.log("BUFOR B", b);
console.log("BUFOR AB", ab);
console.log("CRC", crc);

var a_body = a.slice(10, a.length - 10);
var b_body = b.slice(10, b.length - 10);
var ab_body = ab.slice(10, ab.length - 10);

var gzipHeader = new Buffer([0x1f, 0x8b, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x03]);
var gzipEnding = new Buffer([0x03, 0x00]);

console.log("BODY A", a_body);
console.log("BODY B", b_body);
console.log("BODY AB", ab_body);

var length = gzipHeader.length + a_body.length + b_body.length + gzipEnding.length + crc.combinedCrc32.length + crc.bufferLength.length;
var end = 0;
var d = new Buffer(length);

gzipHeader.copy(d, end);
end += gzipHeader.length;

a_body.copy(d, end);
end += a_body.length;

b_body.copy(d, end);
end += b_body.length;

gzipEnding.copy(d, end);
end += gzipEnding.length;

crc.combinedCrc32.copy(d, end);
end += crc.combinedCrc32.length;

crc.bufferLength.copy(d, end);
end += crc.bufferLength.length;

console.log("DLUGOSC", length, end);
console.log(d);

console.log("ODKOMPRES", compress_buffer.uncompress(d));
