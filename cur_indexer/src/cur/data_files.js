var csvToJson = require("csvtojson"),
  fs = require("fs"),
  hl = require("highland"),
  zlib = require("zlib");

function getLocalFileStream(filePath) {
  return fs.createReadStream(filePath);
}

function getUnzippedStream(filePath) {
  var stream = /^s3\:\\\\/.test(filePath)
    ? getS3Stream(filePath)
    : getLocalFileStream(filePath);
  return stream.pipe(zlib.createGunzip());
}

function parseCsvStream(headers, stream) {
  var csvStream = csvToJson(
    {
      headers: headers,
      noheader: false,
      checkType: true
    },
    { objectMode: true }
  ).fromStream(stream);

  return hl(csvStream);
}

function getDataStream(options) {
  return hl(options.manifest.files)
    .map(getUnzippedStream)
    .flatMap(function(stream) {
      return parseCsvStream(options.manifest.columns, stream);
    });
}

module.exports = { getDataStream: getDataStream };
