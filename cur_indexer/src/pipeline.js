const hl = require("highland");

function start({ batchSize, concurrency, consumeBatchAsync }, dataStream) {
  let batchNum = 0;
  dataStream
    .batch(batchSize)
    .map(items => {
      batchNum++;
      return hl(consumeBatchAsync(items, { batchNum }));
    })
    .parallel(concurrency)
    .errors(args => {
      hl.log(args);
    })
    .done(() => {
      hl.log("done indexing");
    });
}

module.exports = {
  start
};
