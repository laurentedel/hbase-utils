# hbase-compaction

HBase lacks a progress indicator of a table major compaction.
This class aims to present a compaction progress by scanning the KVs compacted and provides an ETA (which is clearly not accurate though).

Build using mvn package, and execute like (easiest way):

```
[root@sandbox]# cd /usr/hdp/current/hbase-client
[root@sandbox]# export HBASE_CLASSPATH=${HBASE_CLASSPATH}:./hbase-compactions-0.1-SNAPSHOT.jar
[root@sandbox]# hbase com.ledel.hbase.Compact --table=TestTable
``` 

I've included a dirty *maven-antrun-plugin* in the pom.xml so it upload directly in hbase-client of the sandbox, just remove the `build` section of the pom.xml if you don't want that!

Happy compacting!