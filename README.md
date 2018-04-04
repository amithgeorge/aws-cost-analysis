## Usage

```
AWS_PROFILE=prod \
ES_URL=http://localhost:32775 \
CUR_NAME=some_report_name \
CUR_S3_BUCKET=some_s3_bucket \
CUR_S3_PREFIX=some_prefix_for_the_report \
node index.js download-files --year 2018 --month 3

AWS_PROFILE=prod \
ES_URL=http://localhost:32775 \
CUR_NAME=some_report_name \
CUR_S3_BUCKET=some_s3_bucket \
CUR_S3_PREFIX=some_prefix_for_the_report \
node index.js index-data --year 2018 --month 3
```

## Logical areas

### Find latest manifest

* Get the column names
* Lowercase the names, replace `:` and `-` with `_`, add suffix to create unique
  names
* Get the actual file paths

### Download the files

* Download and unzip the files
* Parse the csv data into individual json records

### Send to ES

* Based on the month, compute the index name.
* Delete the index if it exists.
* Create the index. Or not.
* Send each record to ES. Maybe as a bulk request
