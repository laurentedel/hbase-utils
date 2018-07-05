package com.ledel.hbase;

import org.apache.commons.cli.*;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;


/**
 * Random data generator
 * Specify the number of rows, column families, column qualifier and data length
 * <p>
 * Usage: hadoop jar <jarname> <tablename>
 * or export jar in hbase client classpath and:
 * hbase com.ledel.hbase.Generator --table=<TABLENAME> --rows=1000 --columnfamilies=1 --columns=12
 *
 */
public class Generator {
  private static final Log LOG = LogFactory.getLog(Generator.class);

  public static void main(String[] args) throws IOException {

    Logger.getRootLogger().setLevel(Level.INFO);
    // create the command line parser
    CommandLineParser parser = new GnuParser();

    // create the Options
    Options options = new Options();

    Option t = new Option("t", "table", true, "HBase table to be created");
    t.setRequired(true);
    options.addOption(t);

    Option r = new Option("r", "rows", true, "number of rows to be inserted");
    t.setRequired(true);
    options.addOption(r);

    Option cf = new Option("cf", "columnfamilies", true, "number of ColumnFamilies");
    t.setRequired(true);
    options.addOption(cf);

    Option q = new Option("q", "columns", true, "number of Columns in each ColumnFamily");
    t.setRequired(true);
    options.addOption(q);

    HelpFormatter formatter = new HelpFormatter();
    CommandLine cmd = null;
    try {
      cmd = parser.parse(options, args);
    } catch (ParseException e) {
      formatter.printHelp("com.ledel.hbase.Generator -t=TABLE -r=n -cf=n -q=n", options);
      e.printStackTrace();
      System.exit(1);
    }

    Configuration config = HBaseConfiguration.create();
    Connection conn = ConnectionFactory.createConnection(config);
    TableName TABLE_NAME = TableName.valueOf(cmd.getOptionValue("table"));
    int columnFamilies = Integer.parseInt(cmd.getOptionValue("cf"));
    int columnQualifiers = Integer.parseInt(cmd.getOptionValue("q"));
    int rows = Integer.parseInt(cmd.getOptionValue("r"));

    HTableDescriptor hTable = new HTableDescriptor(TABLE_NAME);
    hTable.setConfiguration("hbase.table.sanity.checks", "false");

    Admin admin = conn.getAdmin();
    if (admin.tableExists(TABLE_NAME)) {
      LOG.fatal("Table " + TABLE_NAME + "already exists");
      System.exit(0);
    } else {
      for (int j = 0; j < columnFamilies; j++) {
        hTable.addFamily(new HColumnDescriptor("cf" + j));
      }
      admin.createTable(hTable);
    }

    Table table = conn.getTable(TABLE_NAME);
    List<Put> puts = new ArrayList<Put>();

    for (int i = 0; i < rows; i++) {
      puts.clear();
      Put p = new Put(Bytes.toBytes("row" + i));

      for (int j = 0; j < columnFamilies; j++) {
        for (int k = 0; k < columnQualifiers; k++) {
          String uuid = UUID.randomUUID().toString();
          p.addColumn(Bytes.toBytes("cf" + j), Bytes.toBytes("cq" + k), Bytes.toBytes(uuid));
          puts.add(p);
          LOG.debug("added (row " + i + ", cf " + j + ", cq " + k + ") to put");
        }
      }
      table.put(puts);
      LOG.info("inserted " + puts.size() + "columns in a row");
    }

    conn.close();

  }

}
