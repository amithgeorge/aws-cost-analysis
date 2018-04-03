var AWS = require("aws-sdk"),
  curDate = require("./date"),
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

function columnNames(columns) {
  var getUniqueColumnName = makeUniqueColumnNameGetter();

  return columns.map(columnToName).map(getUniqueColumnName);
}

function s3DataFilePaths(bucket, files) {
  return files.map(function(key) {
    return "s3://" + bucket + "/" + key;
  });
}

function sanitizeManifest(manifest) {
  manifest.columns = columnNames(manifest.columns);
  manifest.files = s3DataFilePaths(manifest.bucket, manifest.reportKeys);
  return manifest;
}

function getLatestManifestPath(options) {
  var s3Prefix = options.s3Prefix,
    reportName = options.reportName,
    manifestS3Path = options.s3Prefix + options.reportName + "-Manifest.json";

  return "";
}

function downloadManifest(options) {
  var manifestS3Path = getLatestManifestPath(options);
}

function getManifestFromLocalFile(options) {
  return JSON.parse(fs.readFileSync(options.filePath));
}

function getManifestFromS3(options) {
  return null;
}

function manifestName(reportName) {
  return `${reportName}-Manifest.json`;
}

function readManifestFromLocal(options) {
  var manifest = options.filePath
    ? getManifestFromLocalFile(options)
    : getManifestFromS3(options);

  manifest = sanitizeManifest(manifest);
  return manifest;
}

function getManifestS3Key({ s3Bucket, prefix, reportName, year, month }) {
  let period = curDate.getPeriodStr(year, month);
  return `${prefix}/${reportName}/${period}/${manifestName(reportName)}`;
}

function getManifestLocalPath({ reportName, year, month }) {
  let localFilePrefix = curDate.getLocalFilePrefix(year, month);
  let localPath = path.resolve(
    `./data_files/${localFilePrefix}/${manifestName(reportName)}`
  );
  return localPath;
}

module.exports = {
  readManifestFromLocal,
  getManifestS3Key,
  getManifestLocalPath
};
