package com.ledel.hbase;

import org.apache.commons.cli.*;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.protobuf.generated.AdminProtos.GetRegionInfoResponse.CompactionState;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;


/**
 * Compact a HBase table and display the percentage done
 * For that, we're scanning all regions CompactionState: every region compacted
 * which has its status going from MAJOR to NONE is considered as compacted
 * <p>
 * Usage: hadoop jar <jarname> <tablename>
 * or export jar in hbase client classpath and: hbase com.ledel.hbase.Compact <TABLENAME>
 */
public class Compact {
  private static final Log LOG = LogFactory.getLog(Compact.class);

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

    System.out.print(string);
  }

  public static void main(String[] args) {

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
      formatter.printHelp("com.ledel.hbase.Compact -t=TABLE", options);
      e.printStackTrace();
      System.exit(1);
    }

    TableName TABLE_NAME = TableName.valueOf(cmd.getOptionValue("table"));
    Configuration config = HBaseConfiguration.create();

    try {
      Connection conn = ConnectionFactory.createConnection(config);
      Admin admin = conn.getAdmin();

      // REGION LEVEL: GET COMPACTION PROGRESS FOR EACH REGION
      ClusterStatus cs;
      HashMap<String, Long> regionCompactionProgress = new HashMap<String, Long>();

      if (!admin.tableExists(TABLE_NAME)) {
        LOG.fatal("Table " + args[0] + "does not exists");
        System.exit(0);
      }
      admin.majorCompact(TABLE_NAME);

      List<HRegionInfo> regions = admin.getTableRegions(TABLE_NAME);
      int totalRegions = regions.size();
      int hasEndedCompactingRegion = 0;
      long startTime = System.currentTimeMillis();

      while (hasEndedCompactingRegion < totalRegions) {
        cs = admin.getClusterStatus();
        for (ServerName server : cs.getServers()) {
          ServerLoad load = cs.getLoad(server);
          for (Map.Entry<byte[], RegionLoad> entry : load.getRegionsLoad().entrySet()) {
            RegionLoad regionLoad = entry.getValue();
            String regionName = regionLoad.getNameAsString();
            HashMap<Long, Long> KVs = new HashMap<Long, Long>();
            KVs.put(regionLoad.getCurrentCompactedKVs(), regionLoad.getTotalCompactingKVs());
            Long compactionProgress = 0L;
            if (regionLoad.getTotalCompactingKVs() > 0) {
              compactionProgress = regionLoad.getCurrentCompactedKVs() * 100 / regionLoad.getTotalCompactingKVs();
            }
            regionCompactionProgress.put(regionName, compactionProgress);
          }
        }

        hasEndedCompactingRegion = 0;
        Long totalProgress = 0L;
        for (HRegionInfo regionInfo : regions) {
          if (admin.getCompactionStateForRegion(regionInfo.getRegionName()).equals(CompactionState.NONE)) {
            hasEndedCompactingRegion++;
            //System.out.println("Region " + regionInfo.getEncodedName() + " has compacted");
            totalProgress += 100;
          } else {
            // if region is still to be compacted then 100% compacted should not be taken as is (compaction will still occur)
            if (regionCompactionProgress.get(regionInfo.getRegionNameAsString()) < 100) {
              totalProgress += regionCompactionProgress.get(regionInfo.getRegionNameAsString());
            }
          }
        }

        printProgress(startTime, 100, totalProgress / (regions.size()));
        Thread.sleep(1000);
      }

      System.out.println("\ndone!\n");
      admin.close();

    } catch (IOException e1) {
      // TODO Auto-generated catch block
      e1.printStackTrace();
    } catch (InterruptedException e1) {
      // TODO Auto-generated catch block
      e1.printStackTrace();
    }
  }

}
