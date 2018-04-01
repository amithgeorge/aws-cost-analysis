var config = require("./config"),
  curDataFiles = require("./cur/data_files"),
  curManifest = require("./cur/manifest"),
  csvToJson = require("csvtojson"),
  elasticsearch = require("elasticsearch"),
  esIndexer = require("./es/indexer"),
  fs = require("fs"),
  path = require("path"),
  stream = require("stream"),
  athenaCreateQuery = require("./athena/queries/create_table");

// var manifestPath = "s3://qt-awscostice/qt/QT-ICE/20180301-20180401/QT-ICE-Manifest.json"

let qualifiedTableName = "aws_costs.cost_usage_reports",
  location = "s3://qt-awscostice/qt/hdfs/cur/";

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

function makeESClient() {
  let client = new elasticsearch.Client({
    host: config.getValue("ES_URL"),
    apiVersion: "6.2"
  });

  return client;
}

function makeIndexer() {
  let client = makeESClient();
  return esIndexer.makeIndexer(client);
}

function doSomething() {
  let manifest = curManifest.getManifest({
    filePath: path.resolve("./data_files/QT-ICE-Manifest.json")
  });
  manifest.files = [path.resolve("./data_files/QT-ICE-1.csv.gz")];

  let index = "my_temp_index";
  let indexer = makeIndexer();

  indexer
    .recreateIndex({ index: index, options: { numShards: 1, numReplicas: 0 } })
    .then(() => {
      curDataFiles
        .getDataStream({ manifest: manifest })
        .batch(100)
        .map(items =>
          console.log(
            new Date().toISOString() + ": processing " + items.length + " items"
          )
        )
        .done(() => {
          console.log("done indexing");
        });
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
