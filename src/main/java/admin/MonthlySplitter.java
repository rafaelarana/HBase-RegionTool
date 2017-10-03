package admin;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by rarana on 02/10/2017.
 */
public class MonthlySplitter {

    private static final Log LOG = LogFactory.getLog(MonthlySplitter.class);

    TableName tableName;
    Connection connection;
    Configuration conf;
    boolean isReport = false;


    public  MonthlySplitter(Connection aConnection, TableName aTableName, Configuration aConf,boolean isReport) {

        this.connection = aConnection;
        this.tableName = aTableName;
        this.conf= aConf;
        this.isReport = isReport;

    }

    /**
     * Split the region after the boundary date  provided using  the previous month as reference. So if splitPoint is
     * 201711 it will ad a new set of regions starting in 201711 using the region split of all regions of 201710.
     *
     * @param splitPoint year and month ( format YYYYMM) used a boundary to clone the region split.
     * @throws IOException
     */
    public void split(String splitPoint) throws IOException {

        Admin admin = this.connection.getAdmin();

        List<HRegionInfo> tableRegions = admin.getTableRegions(tableName);
        StageBuilder stageBuilder = new StageByDateBuilder(tableName, tableRegions, conf);
        try {
            stageBuilder.setStageBoundaries();
        } catch (IOException e) {
            LOG.error("Cannot parse splits for table " + tableName + " Cause:" + e.getCause());
            LOG.error(e.getStackTrace());
        }

        // Get the list of regions of the previous month as pattern
        List<HRegionInfo> lastMonthList = stageBuilder.getLastMonthList(StageByDateBuilder.getPreviousMonth(splitPoint));

        RegionsUtil.printRegionInfo(lastMonthList);


        // Gets the list of split points replacing the first part of the key with the prefix
        List<String> splitPointsList = extractSplitPoints(lastMonthList,splitPoint);

        for (String point : splitPointsList) {
            LOG.debug("Splitting table " + tableName + " at split point " + point);

            if (!isReport ) {
                admin.split(this.tableName, Bytes.toBytes(point));
                try {
                    Thread.sleep(30*1000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }

                RegionsUtil.checkInTransition(admin,30*1000);
            }

        }

        admin.close();

    }

    /**
     * Builds a list of split points using a the start key of list of regions
     * @param lastMonthList the list of regions to extract the split points (start key)
     * @param newPrefix the date prefix used to replace the original one. Must follow th format YYYYMM
     */
    private List<String> extractSplitPoints(List<HRegionInfo> lastMonthList, String newPrefix) {

        if (newPrefix ==  null || newPrefix.length() != 6 ) {
            throw new IllegalArgumentException("Prefix (" + newPrefix + ")is invalid. Valid format: 6 characters, format YYYYDD");
        }

        List<String> newPrefixList = new ArrayList<>();

        for (HRegionInfo info : lastMonthList) {
            newPrefixList.add(newPrefix +
                    (Bytes.toString(info.getStartKey()).substring(6)));

        }
        return newPrefixList;
    }


}
