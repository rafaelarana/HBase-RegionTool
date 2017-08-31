package admin;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileContext;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.RegionLocator;
import org.apache.hadoop.hbase.master.RegionState;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Scanner;

// cc ClusterOperationExample Shows the use of the cluster operations
public class DeleteRegions {


    private static final Log LOG = LogFactory.getLog(DeleteRegions.class);

    private static int waitTime = 60 * 1000;

    public static void main(String[] args) throws IOException, InterruptedException {


        String sTable = args[0];
        String splitKey = args[1];
        boolean splitBorderOn  = true;

        Configuration conf = HBaseConfiguration.create();

        Connection connection = ConnectionFactory.createConnection(conf);
        Admin admin = connection.getAdmin();

        TableName tableName = TableName.valueOf(sTable);

        RegionLocator locator = connection.getRegionLocator(tableName);

        HRegionInfo borderlineRegion = StageByDateBuilder.getRegion(locator, splitKey);


        if (null == borderlineRegion ) {
            System.out.print("No region found for split key [" + splitKey + "] and  table [" + sTable + "]");
            return;
        }

        System.out.println("Border Line Region: " + borderlineRegion.getRegionNameAsString()
                + " Start Key:" + Bytes.toString(borderlineRegion.getStartKey())
                + " End Key: " + Bytes.toString(borderlineRegion.getEndKey()));

        System.out.println("Split Key: " + splitKey);

        //System.out.println((Bytes.toString(borderlineRegion.getStartKey()).equals(splitKey)));
        //System.out.println((Bytes.toString(borderlineRegion.getEndKey()).equals(splitKey)));

        boolean borderSplitted = false;
        if ( splitBorderOn &&
                 !(Bytes.toString(borderlineRegion.getStartKey()).equals(splitKey)) &&
                !(Bytes.toString(borderlineRegion.getEndKey()).equals(splitKey)) ) {

            System.out.println("Spitting region " + borderlineRegion  + " with split Key: " + splitKey);


            admin.splitRegion(borderlineRegion.getRegionName(), Bytes.toBytes(splitKey));
            borderSplitted = true;

            checkInTransition(admin);
        }

        List<HRegionInfo> regions = new ArrayList<>();
        List<HRegionInfo> tableRegions = admin.getTableRegions(tableName);
        StageByDateBuilder rsp = new StageByDateBuilder(tableName, tableRegions);

        try {

            regions = rsp.getRegionsToArchive(splitKey);

        } catch (IOException e) {
            System.out.println("Cannot parse splits for table " + tableName + " Cause:" + e.getCause());
            LOG.error(e.getStackTrace());

        }


        if (regions.size() == 0 ) {
            System.out.println("WARNING: No regions to delete...");
            return;

        }

        System.out.println("WARNING:The following Regions will be deleted permanently...");

        printRegionInfo(regions);

        Scanner input = new Scanner(System.in);
        System.out.println("Are you sure you want to remove all the data in that regions(Y/N)?: ");
        String s = input.next(); // getting a String value

        if (!s.equalsIgnoreCase("Y")) {
            System.out.println("ABORTING operation. No region deleted.");
            return;
        }


        // Clean HDFS
        for (HRegionInfo info : regions) {

            admin.offline(info.getRegionName());
            deleteRegionFromHDFS(conf, info);
            admin.assign(info.getRegionName());


            checkInTransition(admin);


        }

        // Merge Regions
        HRegionInfo previous = null;
        HRegionInfo current;

        for (HRegionInfo info : regions) {

            current = info;

            if (previous != null) {
                System.out.println("MERGING REGIONS:" + previous + " with " + current);
                admin.mergeRegions(previous.getEncodedNameAsBytes(), current.getEncodedNameAsBytes(), false);

                checkInTransition(admin);

                RegionLocator tLocator = connection.getRegionLocator(tableName);
                previous = StageByDateBuilder.getRegion(tLocator, Bytes.toString(current.getStartKey()), true);
                System.out.println("New Region:" + previous);

                checkInTransition(admin);


            } else {
                previous = current;
            }


        }


        Map<String,RegionState> regMap = admin.getClusterStatus().getRegionsInTransition();
        System.out.println(regMap.keySet());



        if  ( borderSplitted ) {
            // It needs to refresh the borderLineRegion
            borderlineRegion = getRegionWithKey(connection, tableName, splitKey);
        }

        System.out.println("MERGING REGIONS:" + previous.getRegionNameAsString() + " with " + borderlineRegion.getRegionNameAsString());
        admin.mergeRegions(previous.getEncodedNameAsBytes(), borderlineRegion.getEncodedNameAsBytes(),true);

        admin.close();
        connection.close();


    }

    private static void  checkInTransition(Admin admin) throws IOException {

        boolean inTransitions = true;
        Map<String,RegionState> regMap = admin.getClusterStatus().getRegionsInTransition();

        while (inTransitions) {

            System.out.println("Sleeping 10 sec. until no Region Server in transition....");
            try {
                Thread.sleep(10 * 1000L);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            regMap = admin.getClusterStatus().getRegionsInTransition();
            if (regMap.keySet().isEmpty()) {
                inTransitions = false;
            }

        }


    }

    public static HRegionInfo getRegionWithKey (Connection conn, TableName table, String key) throws IOException {

        RegionLocator tLocator = conn.getRegionLocator(table);
        return StageByDateBuilder.getRegion(tLocator, key, true);

    }


    private static void deleteRegionFromHDFS(Configuration conf, HRegionInfo regionInfo) throws IOException {

        System.out.println("Deleting region " + regionInfo.getRegionNameAsString() +" from HDFS");

        FileContext fc = FileContext.getFileContext(conf);
        Path hbaseRoot = new Path(conf.get("hbase.rootdir"));
        String region = regionInfo.getRegionNameAsString();


        String[] regionComps = region.split(",");
        String regionId = regionComps[2].split("\\.")[1];
        String tableName = regionComps[0];
        if (regionId.isEmpty()) throw new IllegalStateException("regionId must be null: " + region);

        //Build the path
        // if region name has comma, we're dead
        Path regionPath = new Path(hbaseRoot, "data" + Path.SEPARATOR
                + regionInfo.getTable().getNamespaceAsString() + Path.SEPARATOR
                + tableName + Path.SEPARATOR + regionId);
        System.out.println("DELETING REGION HDFS PATH: " + regionPath);

        // Delete recursive
        fc.delete(regionPath, true);


    }

    /*
    private void waitAndVerifyRegionNum(HMaster master, TableName tablename,
                                        int expectedRegionNum) throws Exception {
        List<Pair<HRegionInfo, ServerName>> tableRegionsInMeta;
        List<HRegionInfo> tableRegionsInMaster;
        long timeout = System.currentTimeMillis() + waitTime;
        while (System.currentTimeMillis() < timeout) {
            tableRegionsInMeta = MetaTableAccessor.getTableRegionsAndLocations(
                    TEST_UTIL.getConnection(), tablename);
            tableRegionsInMaster = master.getAssignmentManager().getRegionStates()
                    .getRegionsOfTable(tablename);
            if (tableRegionsInMeta.size() == expectedRegionNum
                    && tableRegionsInMaster.size() == expectedRegionNum) {
                break;
            }
            Thread.sleep(250);
        }

        tableRegionsInMeta = MetaTableAccessor.getTableRegionsAndLocations(
                TEST_UTIL.getConnection(), tablename);
        LOG.info("Regions after merge:" + Joiner.on(',').join(tableRegionsInMeta));
        assertEquals(expectedRegionNum, tableRegionsInMeta.size());
    }

    */

    private static void printRegionInfo(List<HRegionInfo> infos) {
        for (HRegionInfo info : infos) {
            System.out.println(" Region: " + info.getRegionNameAsString()
                    + " Start Key:" + Bytes.toString(info.getStartKey())
                    + " End Key: " + Bytes.toString(info.getEndKey()));
        }
    }
}
