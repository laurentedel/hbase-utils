package com.ledel.hbase;

import java.io.*;
import java.util.List;

import org.apache.commons.cli.*;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.Store;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * A class that returns every midpoint of every region
 * of a given HBase table. That will allow to split
 * a specific region because you need a split point
 * <p>
 * Usage: hadoop jar <jarname> <tablename>
 * or export jar in hbase client classpath and: hbase com.ledel.hbase.GetMidPoint <TABLENAME>
 */
public class GetMidPoint {
    private static final Log LOG = LogFactory.getLog(GetMidPoint.class);

    public static void main(String[] args) throws IOException {

        // create the command line parser
        CommandLineParser parser = new GnuParser();

        // create the Options
        Options options = new Options();

        Option t = new Option("t", "table", true, "HBase table to be parsed");
        t.setRequired(true);
        options.addOption(t);

        Option c = new Option("c", "cf", true, "use a specific ColumnFamily only");
        c.setRequired(false);
        options.addOption(c);

        Option r = new Option("r", "region", true, "use a specific ENCODED_REGION_NAME only");
        r.setRequired(false);
        options.addOption(r);

        HelpFormatter formatter = new HelpFormatter();
        CommandLine cmd = null;
        try {
            cmd = parser.parse(options, args);
        } catch (ParseException e) {
            formatter.printHelp("com.ledel.hbase.getMidPoint -t=TABLE [-c=COLUMNFAMILY] [-r=REGION_ENCODED_NAME]", options);
            e.printStackTrace();
            System.exit(1);
        }

        TableName TABLE_NAME = TableName.valueOf(cmd.getOptionValue("table"));
        String COLUMN_FAMILY = cmd.getOptionValue("cf", "");
        String REGION_ENCODED_NAME = cmd.getOptionValue("region", "");

        PrintStream out = new PrintStream(new FileOutputStream("splitpoints.csv"));
        System.setOut(out);
        Configuration config = HBaseConfiguration.create();

        Connection conn = ConnectionFactory.createConnection(config);
        Admin admin = conn.getAdmin();

        if (!admin.tableExists(TABLE_NAME)) {
            LOG.fatal("Table " + args[0] + "does not exists");
            System.exit(0);
        }

        Table table = conn.getTable(TABLE_NAME);
        List<HRegionInfo> regions = admin.getTableRegions(TABLE_NAME);
        HRegionInfo theRegion = null;

        LOG.info("list regions: " + regions);
        //TODO test if the region exists...
        if (REGION_ENCODED_NAME != "") {
            for (HRegionInfo hRegionInfo : regions) {
                LOG.debug("hRegionInfo " + hRegionInfo.getEncodedName() + " / " + REGION_ENCODED_NAME);
                if (hRegionInfo.getEncodedName().compareTo(REGION_ENCODED_NAME) == 0) {
                    LOG.info("0");
                    theRegion = hRegionInfo;
                }
            }
            regions.clear();
            regions.add(theRegion);
        }

        System.out.println("REGION NAME,SPLIT POINT,STORE SIZE,COLUMN FAMILY");

        for (HRegionInfo hRegionInfo : regions) {
            LOG.info("Full region name: " + hRegionInfo.getRegionNameAsString());
            HRegion region = HRegion.openHRegion(hRegionInfo, table.getTableDescriptor(), null, config);

            Store theStore = null;
            List<Store> stores = region.getStores();

            //TODO test if the CF exists !!
            if (COLUMN_FAMILY != "") {
                for (Store store : stores) {
                    LOG.debug("store " + store.getColumnFamilyName() + " / " + COLUMN_FAMILY);
                    if (store.getColumnFamilyName().compareTo(COLUMN_FAMILY) == 0) {
                        LOG.info("compare=0");
                        theStore = store;
                    }
                }
                stores.clear();
                stores.add(theStore);
            }

            LOG.info(stores.size() + " stores (Column Families) found: " + stores.toString());

            for (Store store : stores) {
                byte[] splitPoint = store.getSplitPoint();
                String split = Bytes.toString(splitPoint);
                System.out.println(store.getRegionInfo().getEncodedName()
                        + "," + split
                        + "," + store.getSize()
                        + "," + store.getColumnFamilyName());
            }
        }

        table.close();
        admin.close();
    }
}
