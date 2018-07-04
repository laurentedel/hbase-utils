# HBase GetSplits

This class is here to get the mid keys of all regions of a table.
If you want to split a HBase table region, you need to specify the split key, which you probably don't know.

## Using

I usually export the jar from Eclipse, copy the jar on your server.

```
$ export HBASE_CLASSPATH=${HBASE_CLASSPATH}:./hbase-getsplits-0.1-SNAPSHOT.jar
$ hbase com.ledel.hbase.GetMidPoint TABLE [COLUMNFAMILY]
```

The results will be exported in the dir you're executing the jar from, in splitpoints.csv file

If you don't specify a CF, all CF will be printed (the mid key isn't probably the same)
You'll also have the store size. 

## Building

Run `mvn package` to build.
I've included a dirty *maven-antrun-plugin* in the pom.xml so it upload directly in hbase-client of the sandbox, just remove the `build` section of the pom.xml if you don't want that!



## License

hbase-getmidpoint is not licenced, go ahead :)
