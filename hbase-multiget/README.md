# hbase-multiget

This is an example of a batch GET on several rows instead of having a GET per rowkey.
It returns the total size of row cells as well. 

Put all the rowkeys (one per line) into a `gets.txt` file on current directory, build using `mvn package`, and execute like (easiest way):

```
[root@sandbox]# cd /usr/hdp/current/hbase-client
[root@sandbox]# export HBASE_CLASSPATH=${HBASE_CLASSPATH}:./hbase-multiget-0.1-SNAPSHOT.jar
[root@sandbox]# hbase com.ledel.hbase.MultiGet --table=TestTable
``` 

I've included a dirty *maven-antrun-plugin* in the pom.xml so it upload directly in hbase-client of the sandbox, just remove the `build` section of the pom.xml if you don't want that!
