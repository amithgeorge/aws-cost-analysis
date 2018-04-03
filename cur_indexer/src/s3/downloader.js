const fs = require("fs"),
  mkdirp = require("mkdirp");

function closeAndDelete(stream, path, cb) {
  if (!cb) {
    cb = () => {};
  }
  stream.close();
  fs.unlink(path, cb);
}

function createWriteStream(path) {
  let fileStream = fs.createWriteStream(path);
  return {
    stream: fileStream,
    delete: cb => {
      closeAndDelete(fileStream, path, cb);
    }
  };
}

class S3Downloader {
  constructor(s3Client) {
    this.s3Client = s3Client;
  }

  downloadFileAsync({ localPath, s3Bucket, s3Key }) {
    return new Promise((resolve, reject) => {
      let params = { Bucket: s3Bucket, Key: s3Key };

      let localFile = createWriteStream(localPath);
      let fileStream = localFile.stream;
      fileStream.on("error", err => {
        console.error(`Error downloading ${s3Key}`, err);
        localFile.delete();
        reject({ errorMessage: `Error downloading ${s3Key}`, error: err });
      });
      fileStream.on("finish", () => {
        console.log(`Successfully downloaded ${s3Key}`);
        resolve({ localPath });
      });

      let inputStream = this.s3Client
        .getObject(params)
        .createReadStream()
        .on("error", err => {
          console.error("Error streaming file from S3.", params, err);
          localFile.delete();
          reject({
            errorMessage: "Error streaming file from S3.",
            s3Params: params,
            error: err
          });
        });

      console.log(
        `Downloading ${s3Key} to ${localPath} from bucket ${s3Bucket}`
      );
      inputStream.pipe(fileStream);
    });
  }
}

module.exports = {
  S3Downloader
};
