package com.ledel.hbase;

import org.apache.commons.cli.*;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.protobuf.generated.AdminProtos.GetRegionInfoResponse.CompactionState;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.TimeUnit;


/**
 * MultiGet example
 * <p>
 * Usage: hadoop jar <jarname> <tablename>
 * or export jar in hbase client classpath and: hbase com.ledel.hbase.MultiGet --table=<TABLENAME>
 *
 * the rows to be get has to be written in gets.txt file in the base directory (1 rowkey per line)
 */
public class MultiGet {
  private static final Log LOG = LogFactory.getLog(MultiGet.class);

   public static void main(String[] args) throws IOException {

    // create the command line parser
    CommandLineParser parser = new GnuParser();

    // create the Options
    Options options = new Options();

    Option t = new Option("t", "table", true, "HBase table to be parsed");
    t.setRequired(true);
    options.addOption(t);

    HelpFormatter formatter = new HelpFormatter();
    CommandLine cmd = null;
    try {
      cmd = parser.parse(options, args);
    } catch (ParseException e) {
      formatter.printHelp("com.ledel.hbase.MultiGet -t=TABLE", options);
      e.printStackTrace();
      System.exit(1);
    }

     List<String> list = FileUtils.readLines(new File("gets.txt"), "utf-8");
     System.out.println("LIST: " + list.toString());

    Configuration config = HBaseConfiguration.create();
    Connection conn = ConnectionFactory.createConnection(config);
    TableName TABLE_NAME = TableName.valueOf(cmd.getOptionValue("table"));
    Table table = conn.getTable(TABLE_NAME);

    try {
      List<Get> gets = new ArrayList<Get>();
      for(String index : list) {
        gets.add(new Get(Bytes.toBytes(index)));
      }

      Result[] results = table.get(gets);
      for (Result result : results) {
        String row = Bytes.toString(result.getRow());
        //String value = Bytes.toString(result.getValue(Bytes.toBytes("info"), Bytes.toBytes("0")));
        //System.out.println("ROW " + row + " = " + value);
        NavigableMap<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> map = result.getMap();
        for (Map.Entry<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> navigableMapEntry : map.entrySet()) {
          String family = Bytes.toString(navigableMapEntry.getKey());
          System.out.println(StringUtils.repeat("#", row.length()+5));
          System.out.println("ROW:  " + row);
          System.out.println("size: " + Result.getTotalSizeOfCells(result));
          System.out.println("  FAMILY: " + family);
          NavigableMap<byte[], NavigableMap<Long, byte[]>> familyContents = navigableMapEntry.getValue();
          for (Map.Entry<byte[], NavigableMap<Long, byte[]>> mapEntry : familyContents.entrySet()) {
            String qualifier = Bytes.toString(mapEntry.getKey());
            System.out.println("    QUALIFIER: " + qualifier);
            NavigableMap<Long, byte[]> qualifierContents = mapEntry.getValue();
            for (Map.Entry<Long, byte[]> entry : qualifierContents.entrySet()) {
              Long timestamp = entry.getKey();
              String value = Bytes.toString(entry.getValue());
              System.out.printf("      VALUE: %s, %d\n", value, timestamp);
            }
          }
        }
      }
    }
    finally {
      table.close();
      conn.close();
    }

  }

}
