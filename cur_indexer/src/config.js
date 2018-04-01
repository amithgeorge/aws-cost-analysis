var process = require("process");

var cachedConfig = {};

function getValue(key) {
  if (!cachedConfig.hasOwnProperty(key)) {
    cachedConfig[key] = process.env[key];
  }

  return cachedConfig[key];
}

module.exports = {
  getValue: getValue
};
