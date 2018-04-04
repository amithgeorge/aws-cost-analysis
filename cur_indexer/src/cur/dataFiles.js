var csvToJson = require("csvtojson"),
  curDate = require("./date"),
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

function getDataStream({ manifest }) {
  const { dataFilePaths, columns } = manifest;
  const localPaths = dataFilePaths.map(f => f.localPath);
  return hl(localPaths)
    .map(getUnzippedStream)
    .flatMap(function(stream) {
      return parseCsvStream(columns, stream);
    });
}

module.exports = {
  getDataStream
};
