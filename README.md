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
