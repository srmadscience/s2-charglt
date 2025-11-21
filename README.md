# s2-charglt
SingleStore implementation of [Charglt](https://github.com/srmadscience/voltdb-charglt), but on SingleStore

It Has the  basic functionality of charglt, but is missing stuff  - see TODO

## TODO
 
* Feed to downstream systems - https://www.singlestore.com/blog/oracle-real-time-ingestion-overhead-with-kafka/
  SELECT col1, col2, col3 FROM <table_name>
  ORDER BY col1
  INTO KAFKA 'host.example.com:9092/test-topic'
  FIELDS TERMINATED BY ',' ENCLOSED BY '"' ESCAPED BY "\t"
  LINES TERMINATED BY '}' STARTING BY '{';

* user_recent_transactions - needs to be select into kafka
* user_balance needs to be a table/column
* 
## Status

While this is fine to play with, it's not a fair representation of SingleStore at the moment.


