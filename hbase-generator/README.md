# hbase-generator

Quick generate a table with sample data.
You must specify the number of rows, ColumnFamilies and columns to be inserted, the values are UUIDS. 
```
[root@sandbox]# cd /usr/hdp/current/hbase-client
[root@sandbox]# export HBASE_CLASSPATH=${HBASE_CLASSPATH}:./hbase-multiget-0.1-SNAPSHOT.jar
[root@sandbox]# hbase com.ledel.hbase.Generator --table=TestTable1 --rows=5 -cf=2 -q=5
``` 

I've included a dirty *maven-antrun-plugin* in the pom.xml so it upload directly in hbase-client of the sandbox, just remove the `build` section of the pom.xml if you don't want that!
