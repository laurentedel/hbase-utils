# hbase-generator

Quick generate a table with sample data.
You must specify the number of rows, ColumnFamilies and columns to be inserted.
You can optionally set the values length (default 20 characters), the batch size (rows are inserted by batches of 1000 default) and if you want to overwrite a table (default false)

Execute class without parameters to display usage.
 
```
[root@sandbox]# cd /usr/hdp/current/hbase-client
[root@sandbox]# export HBASE_CLASSPATH=${HBASE_CLASSPATH}:./hbase-generator-0.1-SNAPSHOT.jar
[root@sandbox]# hbase com.ledel.hbase.Generator --table=TestTable1 -r=300000 -cf=1 -q=2 [-l=100] [-b=50000] [-o=true]
``` 

I've included a (dirty) *maven-antrun-plugin* in the pom.xml so it upload directly in hbase-client of the sandbox, just remove the `build` section of the pom.xml if you don't want that!

For indication, on my sandbox I'm generating 6000rows/s with 1000 chars long values, 13k rows/s with 100chars.

Compile with ```mvn clean package```
