var AWS = require("aws-sdk"),
  config = require("./config"),
  curDataFiles = require("./cur/dataFiles"),
  curDate = require("./cur/date"),
  curManifest = require("./cur/manifest"),
  csvToJson = require("csvtojson"),
  elasticsearch = require("elasticsearch"),
  esIndexer = require("./es/indexer"),
  fs = require("fs"),
  hl = require("highland"),
  mkdirp = require("mkdirp"),
  path = require("path"),
  pipeline = require("./pipeline"),
  { S3Downloader } = require("./s3/downloader"),
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

function makeS3Client() {
  return new AWS.S3({
    apiVersion: "2006-03-01",
    region: "us-east-1"
  });
}

function makeIndexer() {
  let client = makeESClient();
  return esIndexer.makeIndexer(client);
}

function delayFor2Seconds() {
  return new Promise((resolve, reject) => {
    setTimeout(function() {
      resolve(true);
    }, 2000);
  });
}

function createConsumer(consumeFn) {
  return function(items, { batchNum }) {
    console.log(new Date().toISOString() + ": consuming batch no: " + batchNum);
    return consumeFn(items).then(() => {
      console.log(
        new Date().toISOString() + ": processed batch no: " + batchNum
      );
    });
  };
}

function fakeBatchConsumer() {
  return createConsumer(delayFor2Seconds);
}

function esIndexingBatchConsumer(indexer, { index }) {
  return createConsumer(items => {
    return indexer.bulkIndex({ index: index, data: items });
  });
}

function doSomething() {
  let manifest = getManifest();
  let index = "cur_201802";
  let indexer = makeIndexer();
  let batchSize = 1000;
  let concurrency = 5;

  indexer
    .recreateIndex({ index: index, options: { numShards: 1, numReplicas: 0 } })
    .then(() => {
      let dataStream = curDataFiles.getDataStream({ manifest: manifest });
      pipeline.start(
        {
          batchSize: batchSize,
          concurrency: concurrency,
          consumeBatchAsync: esIndexingBatchConsumer(indexer, { index })
        },
        dataStream
      );
    });
}

//doSomething();

const state = {
  year: 2018,
  month: 3,
  reportName: "QT-ICE",
  s3Bucket: "qt-awscostice",
  prefix: "qt"
};

function repl() {
  let config = require("./src/config"),
    elasticsearch = require("elasticsearch");

  let client = new elasticsearch.Client({
    host: config.getValue("ES_URL"),
    apiVersion: "6.2"
  });

  client.indices.delete({ index: "my_temp_index" });
}

// console.log(
//   athenaCreateQuery({
//     columns: manifest.columns,
//     location: location,
//     qualifiedTableName: qualifiedTableName
//   })
// );
