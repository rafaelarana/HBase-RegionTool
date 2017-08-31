package admin;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.client.coprocessor.Batch;
import org.apache.hadoop.hbase.coprocessor.example.generated.BulkDeleteProtos;
import org.apache.hadoop.hbase.ipc.BlockingRpcCallback;
import org.apache.hadoop.hbase.ipc.ServerRpcController;
import org.apache.hadoop.hbase.master.RegionState;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Scanner;

// cc ClusterOperationExample Shows the use of the cluster operations
public class BulkDelete {


    private static final Log LOG = LogFactory.getLog(BulkDelete.class);
    private static int waitTime = 30 * 1000;

    public static void main(String[] args) throws Throwable {


        String sTable = args[0];
        String splitKey = args[1];


        Scanner input = new Scanner(System.in);

        System.out.println("PLEASE, BACKUP your table before proceed (NOT INCLUDED AS PART OF THIS PROCESS)");
        System.out.println("Are you sure you want to remove all the data from start row \"\" to end row \"" + splitKey + "\"");
        System.out.println("PLEASE CONFIRM (Y/N)?: ");

        String s = input.next(); // getting a String value

        if (!s.equalsIgnoreCase("Y")) {
            System.out.println("ABORTING operation. No data deleted.");
            return;
        }


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

            input = new Scanner(System.in);

            System.out.println("Do you want to Spitting region " + borderlineRegion  + " with split Key: " + splitKey);

            System.out.println("PLEASE CONFIRM (Y/N)?: ");

            s = input.next(); // getting a String value

            if (s.equalsIgnoreCase("Y")) {

                System.out.println("Spitting region " + borderlineRegion  + " with split Key: " + splitKey);


                admin.splitRegion(borderlineRegion.getRegionName(), Bytes.toBytes(splitKey));
                borderSplitted = true;

                checkInTransition(admin);
            }

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


        System.out.println("WARNING:The data in the following regions will be deleted permanently...");

        printRegionInfo(regions);

        Scan scan;
        //scan = new Scan().setFilter(new PrefixFilter(Bytes.toBytes(splitKey)));
        scan = new Scan(Bytes.toBytes(""), Bytes.toBytes(splitKey));
        long noOfRowsDeleted = invokeBulkDeleteProtocol(conf, tableName, scan, 10, BulkDeleteProtos.BulkDeleteRequest.DeleteType.ROW, null);

        System.out.println("ROWS DELETED:" + noOfRowsDeleted);

        if (regions.size() <= 1 ) {
            System.out.println("WARNING: No regions to merge...");
            return;

        }


        input = new Scanner(System.in);

        System.out.println("Do you want to Merge the following regions? ");
        printRegionInfo(regions);

        System.out.println("PLEASE CONFIRM (Y/N)?: ");

        s = input.next(); // getting a String value

        if (s.equalsIgnoreCase("Y")) {
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


        }


        admin.close();
        connection.close();


    }


    private static long invokeBulkDeleteProtocol(Configuration conf, TableName tableName, final Scan scan, final int rowBatchSize,
                                          final BulkDeleteProtos.BulkDeleteRequest.DeleteType deleteType, final Long timeStamp) throws Throwable {
        Table ht = new HTable(conf, tableName);
        long noOfDeletedRows = 0L;
        Batch.Call<BulkDeleteProtos.BulkDeleteService, BulkDeleteProtos.BulkDeleteResponse> callable =
                new Batch.Call<BulkDeleteProtos.BulkDeleteService, BulkDeleteProtos.BulkDeleteResponse>() {
                    ServerRpcController controller = new ServerRpcController();
                    BlockingRpcCallback<BulkDeleteProtos.BulkDeleteResponse> rpcCallback =
                            new BlockingRpcCallback<BulkDeleteProtos.BulkDeleteResponse>();

                    public BulkDeleteProtos.BulkDeleteResponse call(BulkDeleteProtos.BulkDeleteService service) throws IOException {
                        BulkDeleteProtos.BulkDeleteRequest.Builder builder = BulkDeleteProtos.BulkDeleteRequest.newBuilder();
                        builder.setScan(ProtobufUtil.toScan(scan));
                        builder.setDeleteType(deleteType);
                        builder.setRowBatchSize(rowBatchSize);
                        if (timeStamp != null) {
                            builder.setTimestamp(timeStamp);
                        }
                        service.delete(controller, builder.build(), rpcCallback);
                        return rpcCallback.get();
                    }
                };
        Map<byte[], BulkDeleteProtos.BulkDeleteResponse> result = ht.coprocessorService(BulkDeleteProtos.BulkDeleteService.class, scan
                .getStartRow(), scan.getStopRow(), callable);
        for (BulkDeleteProtos.BulkDeleteResponse response : result.values()) {
            noOfDeletedRows += response.getRowsDeleted();
        }
        ht.close();
        return noOfDeletedRows;
    }

    private static void  checkInTransition(Admin admin) throws IOException {

        boolean inTransitions = true;
        Map<String,RegionState> regMap = admin.getClusterStatus().getRegionsInTransition();


        while (inTransitions) {

            System.out.println("Sleeping " + waitTime + " ms. until no Region Server in transition....");
            try {
                Thread.sleep(waitTime);
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

    private static void printRegionInfo(List<HRegionInfo> infos) {
        for (HRegionInfo info : infos) {
            System.out.println(" Region: " + info.getRegionNameAsString()
                    + " Start Key:" + Bytes.toString(info.getStartKey())
                    + " End Key: " + Bytes.toString(info.getEndKey()));
        }
    }
}
