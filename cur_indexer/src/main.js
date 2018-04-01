var fs = require("fs"),
  path = require("path"),
  stream = require("stream"),
  csvToJson = require("csvtojson"),
  through2Batch = require("through2-batch"),
  curManifest = require("./cur/manifest"),
  curDataFiles = require("./cur/data_files"),
  athenaCreateQuery = require("./athena/queries/create_table"),
  qualifiedTableName = "aws_costs.cost_usage_reports",
  location = "s3://qt-awscostice/qt/hdfs/cur/";

// var manifestPath = "s3://qt-awscostice/qt/QT-ICE/20180301-20180401/QT-ICE-Manifest.json"

function getDataFileStream(options) {
  return fs.createReadStream("../data_files/QT-ICE-1.csv");
}

function sendToESTransform() {
  return new stream.Transform({
    tranform: function(batch, encoding, done) {
      console.log(batch);
      done(null, true);
    }
  });
}

function doSomething() {
  var manifest = curManifest.getManifest({
    filePath: path.resolve("./data_files/QT-ICE-Manifest.json")
  });
  manifest.files = [path.resolve("./data_files/QT-ICE-1.csv.gz")];
  var dataStream = curDataFiles.getDataStream({ manifest: manifest });
  dataStream.batch(100).each(function(data) {
    console.log(data);
  });
}

doSomething();

// console.log(
//   athenaCreateQuery({
//     columns: manifest.columns,
//     location: location,
//     qualifiedTableName: qualifiedTableName
//   })
// );
