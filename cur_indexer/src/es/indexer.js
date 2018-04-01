let elasticsearch = require("elasticsearch"),
  config = require("../config"),
  _ = require('lodash');

let client = new elasticsearch.Client({
  host: config.getValue("ES_URL"),
  apiVersion: "6.2"
});

function bulkIndex({ index, data }) {
  let body = 

  let params = {
    refresh: "true",
    index: index
  };
}
