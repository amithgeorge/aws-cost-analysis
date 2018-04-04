let R = require("ramda");

function itemToBulkBodyItems(item) {
  return [{ index: {} }, item];
}

function bulkIndex(client, { index, data }) {
  let body = R.chain(itemToBulkBodyItems)(data);

  let params = {
    index: index,
    type: "lineitem",
    body: body
  };

  return client.bulk(params);
}

function recreateIndex(client, { index, options: { numShards, numReplicas } }) {
  return client.indices
    .delete({ index: index })
    .catch(err => {
      if (err.status === 404) {
        return;
      }
      console.log(err);
    })
    .then(() => {
      return client.indices.create({
        index: index,
        body: {
          settings: {
            number_of_shards: numShards || 1,
            number_of_replicas: numReplicas || 0
          }
        }
      });
    });
}

function makeIndexer(client) {
  return {
    bulkIndex: R.partial(bulkIndex, [client]),
    recreateIndex: R.partial(recreateIndex, [client])
  };
}

module.exports = {
  makeIndexer
};
