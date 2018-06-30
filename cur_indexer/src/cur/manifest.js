var AWS = require("aws-sdk"),
  curDate = require("./date"),
  curDataFiles = require("./dataFiles"),
  fs = require("fs"),
  path = require("path"),
  s3 = new AWS.S3({
    apiVersion: "2006-03-01",
    region: "us-east-1"
  });

function columnToName(column) {
  var name = column.category + "_" + column.name;
  name = name.replace(/\//g, "_");
  name = name.replace(/:/g, "_");
  name = name.replace(/-/g, "_");
  name = name.toLowerCase();
  return name;
}

function makeUniqueColumnNameGetter() {
  var uniqueColumnNames = [],
    columnsTimesSeen = {};

  return function(name) {
    var suffix = "";
    if (columnsTimesSeen[name]) {
      columnsTimesSeen[name] += 1;
    } else {
      columnsTimesSeen[name] = 1;
    }

    if (columnsTimesSeen[name] > 1) {
      suffix = "_" + columnsTimesSeen[name];
    }

    return name + suffix;
  };
}

function columnNames({ columns }) {
  var getUniqueColumnName = makeUniqueColumnNameGetter();

  return columns.map(columnToName).map(getUniqueColumnName);
}

function localDataFilePath(localFilePrefix, fileName) {
  return path.resolve(`./data_files/${localFilePrefix}/${fileName}`);
}

function dataFilePaths(manifest) {
  let localFilePrefix = manifest.billingPeriod.start.substr(0, 6);
  let dataFilePaths = manifest.reportKeys.map(s3Key => {
    let fileName = path.basename(s3Key);
    let localPath = localDataFilePath(localFilePrefix, fileName);
    return {
      localPath,
      s3Key
    };
  });

  return dataFilePaths;
}

function sanitizeManifest(manifest) {
  return Object.assign({}, manifest, {
    columns: columnNames(manifest),
    s3Bucket: manifest.bucket,
    dataFilePaths: dataFilePaths(manifest)
  });
}

function getManifestFromLocalFile(filePath) {
  return JSON.parse(fs.readFileSync(filePath));
}

function manifestName(reportName) {
  return `${reportName}-Manifest.json`;
}

function readLocalManifest({ filePath }) {
  let manifest = getManifestFromLocalFile(filePath);
  return sanitizeManifest(manifest);
}

function getManifestS3Key({ s3Bucket, prefix, reportName, year, month }) {
  let period = curDate.getPeriodStr(year, month);
  return `${prefix}${reportName}/${period}/${manifestName(reportName)}`;
}

function getManifestLocalPath({ reportName, year, month }) {
  let localFilePrefix = curDate.getLocalFilePrefix(year, month);
  return localDataFilePath(localFilePrefix, manifestName(reportName));
}

module.exports = {
  readLocalManifest,
  getManifestS3Key,
  getManifestLocalPath
};
