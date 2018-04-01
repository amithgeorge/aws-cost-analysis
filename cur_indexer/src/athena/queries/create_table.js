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

function columnsToDDL(columns) {
  return columnNames(columns)
    .map(function(name) {
      return "`" + name + "` string";
    })
    .join(",\n");
}

function getCreateTableQuery(options) {
  var queryParts = [];

  queryParts.push(
    "CREATE EXTERNAL TABLE IF NOT EXISTS " + options.qualifiedTableName + "("
  );
  queryParts.push(columnsToDDL(options.columns));
  queryParts.push(")");
  queryParts.push("PARTITIONED BY (dt string)");
  queryParts.push(
    "ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'"
  );
  queryParts.push("WITH SERDEPROPERTIES (");
  queryParts.push("'separatorChar' = ',',");
  queryParts.push("'quoteChar' = '\\\"',");
  queryParts.push("'escapeChar' = '\\\\'");
  queryParts.push(")");
  queryParts.push("LOCATION '" + options.location + "'");
  queryParts.push("TBLPROPERTIES (");
  queryParts.push("'has_encrypted_data'='false', ");
  queryParts.push("'skip.header.line.count'='1'");
  queryParts.push(");");

  return queryParts.join("\n");
}

module.exports = getCreateTableQuery;
