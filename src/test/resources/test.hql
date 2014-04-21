CREATE EXTERNAL TABLE IF NOT EXISTS hbase_t1(key string,v1 map<string,string>,v2 map<string,string>,v3 map<string,string>) 
STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
WITH SERDEPROPERTIES ("hbase.columns.mapping" = ":key,c1:,c2:,c3:")
TBLPROPERTIES("hbase.table.name" = "test");