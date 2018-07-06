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
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * Random data generator
 * Specify the number of rows, column families, column qualifier and data length
 * <p>
 * Usage: hadoop jar <jarname> <tablename>
 * or export jar in hbase client classpath and:
 * hbase com.ledel.hbase.Generator --table=<TABLENAME> --rows=1000 --columnfamilies=1 --columns=12 [--length=20]
 * default length of each column is 20
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
    r.setRequired(true);
    options.addOption(r);

    Option cf = new Option("cf", "columnfamilies", true, "number of ColumnFamilies");
    cf.setRequired(true);
    options.addOption(cf);

    Option q = new Option("q", "columns", true, "number of Columns in each ColumnFamily");
    q.setRequired(true);
    options.addOption(q);

    Option l = new Option("l", "length", true, "length of each value");
    l.setRequired(false);
    options.addOption(l);

    Option b = new Option("b", "batchsize", true, "batch size of puts (default 1000)");
    b.setRequired(false);
    options.addOption(b);

    Option o = new Option("o", "overwrite", true, "overwrite the table (reset all data)");
    o.setRequired(false);
    options.addOption(o);

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
    int length = Integer.parseInt(cmd.getOptionValue("l", "20"));
    int BATCHSIZE = Integer.parseInt(cmd.getOptionValue("b", "1000"));
    boolean overwrite = Boolean.parseBoolean(cmd.getOptionValue("o", "false"));


    HTableDescriptor hTable = new HTableDescriptor(TABLE_NAME);
    //hTable.setConfiguration("hbase.table.sanity.checks", "false");

    Admin admin = conn.getAdmin();
    if (admin.tableExists(TABLE_NAME)) {
      if (!overwrite) {
        LOG.fatal("Table " + TABLE_NAME + " already exists");
        System.exit(0);
      } else {
        admin.disableTable(TABLE_NAME);
        admin.deleteTable(TABLE_NAME);
      }
    }
    for (int j = 0; j < columnFamilies; j++) {
      hTable.addFamily(new HColumnDescriptor("cf" + j));
    }
    admin.createTable(hTable);

    Table table = conn.getTable(TABLE_NAME);
    List<Put> puts = new ArrayList<>();
    puts.clear();

    long start = System.currentTimeMillis();
    for (int i = 0; i < rows; i++) {
      Put p = new Put(Bytes.toBytes("row_" + i));

      for (int j = 0; j < columnFamilies; j++) {
        for (int k = 0; k < columnQualifiers; k++) {
          String value = RandomString.generate(length);
          p.addColumn(Bytes.toBytes("cf" + j), Bytes.toBytes("cq" + k), Bytes.toBytes(value));
          puts.add(p);
          LOG.debug("added (row " + i + ", cf " + j + ", cq " + k + ") to put");
        }
      }

      // Log and insert rows every <BATCHSIZE> rows
      if (i % BATCHSIZE == 0) {
        table.put(puts);
        /* LOG.info(i + "/" + rows + " rows [" + round(i*100/rows) + "%] inserted in " + (System.currentTimeMillis() - start2) + "ms"); */
        printProgress(start, rows, i);
        puts.clear();
      }
    }

    // finish it :)
    table.put(puts);
    long elapsed = System.currentTimeMillis() - start;
    printProgress(start, rows, rows);
    LOG.info(rows + " rows inserted in " + elapsed + "ms (" + (rows * 1000 / elapsed) + "rows/s)");

    conn.close();

  }

  public static class RandomString {
    private static final String ALPHABET = "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ-_ ";
    private static final Random RANDOM = new Random();

    /**
     * Generates random string of given length from Base65 alphabet (numbers, lowercase letters, uppercase letters).
     *
     * @param count length
     * @return random string of given length
     */
    static String generate(int count) {
      StringBuilder sb = new StringBuilder();
      for (int i = 0; i < count; ++i) {
        sb.append(ALPHABET.charAt(RANDOM.nextInt(ALPHABET.length())));
      }
      return sb.toString();
    }
  }

  private static void printProgress(long startTime, long total, long current) {
    long eta = current == 0 ? 0 :
            (total - current) * (System.currentTimeMillis() - startTime) / current;
    String etaHms = current == 0 ? "N/A" :
            String.format("%02d:%02d:%02d", TimeUnit.MILLISECONDS.toHours(eta),
                    TimeUnit.MILLISECONDS.toMinutes(eta) % TimeUnit.HOURS.toMinutes(1),
                    TimeUnit.MILLISECONDS.toSeconds(eta) % TimeUnit.MINUTES.toSeconds(1));

    StringBuilder string = new StringBuilder(140);
    int percent = (int) (current * 100 / total);
    string
            .append('\r')
            .append(String.join("", Collections.nCopies(percent == 0 ? 2 : 2 - (int) (Math.log10(percent)), " ")))
            .append(String.format(" %d%% [", percent))
            .append(String.join("", Collections.nCopies(percent, "=")))
            .append('>')
            .append(String.join("", Collections.nCopies(100 - percent, " ")))
            .append(']')
            .append(String.join("", Collections.nCopies(current == 0 ? (int) (Math.log10(total)) : (int) (Math.log10(total)) - (int) (Math.log10(current)), " ")))
            .append(String.format(" %d/%d, ETA: %s", current, total, etaHms));
    System.out.print("\033[H\033[2J");
    System.out.flush();
    LOG.info(string);
  }
}
