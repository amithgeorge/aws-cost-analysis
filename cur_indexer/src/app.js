// const config = require("./config"),
//   curDate = require("./cur/date"),
//   curManifest = require("./cur/manifest"),
//   mkdirp = require("mkdirp"),
//   path = require("path");

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

function makeS3Client() {
  return new AWS.S3({
    apiVersion: "2006-03-01",
    region: "us-east-1"
  });
}

function ensureDataFilesDir({ year, month }) {
  let localPrefix = curDate.getLocalFilePrefix(year, month);
  return new Promise((resolve, reject) => {
    let localPath = path.resolve(`./data_files/${localPrefix}`);
    mkdirp(localPath, err => {
      if (err) {
        let errorMessage = `Failed to create ${localPath}`;
        console.error(errorMessage);
        reject({ errorMessage, error: err });
        return;
      }

      resolve(null, localPath);
    });
  });
}

function downloadManifest({ s3Bucket, prefix, reportName, year, month }) {
  // sample manifestPath "s3://qt-awscostice/qt/QT-ICE/20180301-20180401/QT-ICE-Manifest.json"

  let s3Key = curManifest.getManifestS3Key({
    s3Bucket,
    prefix,
    reportName,
    year,
    month
  });

  let localPath = curManifest.getManifestLocalPath({ reportName, year, month });
  let s3Downloader = new S3Downloader(makeS3Client());
  return s3Downloader.downloadFileAsync({ localPath, s3Bucket, s3Key });
}

function downloadDataFiles({ year, month, manifest }) {
  let { s3Bucket, dataFilePaths } = manifest;
  let s3Downloader = new S3Downloader(makeS3Client());

  let downloadPromises = dataFilePaths.map(({ s3Key, localPath }) => {
    return s3Downloader.downloadFileAsync({ localPath, s3Bucket, s3Key });
  });

  return Promise.all(downloadPromises);
}

function downloadFiles({ year, month }) {
  const state = {
    year,
    month,
    reportName: config.getValue("CUR_NAME"),
    s3Bucket: config.getValue("CUR_S3_BUCKET"),
    prefix: config.getValue("CUR_S3_PREFIX")
  };

  ensureDataFilesDir(state)
    .then(() => {
      return downloadManifest(state).then(({ localPath }) => {
        return Object.assign({}, state, { manifestPath: localPath });
      });
    })
    .then(state => {
      let { manifestPath } = state;
      let manifest = curManifest.readLocalManifest({
        filePath: manifestPath
      });
      return Object.assign({}, state, { manifest });
    })
    .then(state => {
      return downloadDataFiles(state).then(filePaths => {
        let manifest = Object.assign({}, state.manifest, {
          filePaths: filePaths.map(f => f.localPath)
        });
        return Object.assign({}, state, { manifest });
      });
    })
    .then(state => {
      console.log("Done downloading.", state);
    })
    .catch(err => {
      console.log("Something failed. ", err);
    });
}

function getManifest({ reportName, year, month }) {
  const manifestPath = curManifest.getManifestLocalPath({
    reportName,
    year,
    month
  });
  return curManifest.readLocalManifest({
    filePath: manifestPath
  });
}

function makeESClient() {
  let client = new elasticsearch.Client({
    host: config.getValue("ES_URL"),
    apiVersion: "6.2"
  });

  return client;
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

function esIndexingBatchConsumer(indexer, { index }) {
  return createConsumer(items => {
    return indexer.bulkIndex({ index: index, data: items });
  });
}

function makeIndexer() {
  let client = makeESClient();
  return esIndexer.makeIndexer(client);
}

function indexData({ year, month }) {
  let reportName = config.getValue("CUR_NAME");
  let manifest = getManifest({ reportName, year, month });
  let index = `cur_${curDate.getLocalFilePrefix(year, month)}`;
  let indexer = makeIndexer();
  let batchSize = 50;
  let concurrency = 3;

  indexer
    .recreateIndex({ index: index, options: { numShards: 3, numReplicas: 0 } })
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

module.exports = {
  downloadFiles,
  indexData
};
