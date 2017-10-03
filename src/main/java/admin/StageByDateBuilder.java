package admin;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.RegionLocator;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;

/**
 * Created by Rafaael Arana-Cloudera on 22/03/2017.
 *
 * Implements the StageBuilder to split the whole set of regions of one table in three different stages.
 * The current implementation  assumes 3 stages (Hot, warm and cold) and a key definition including the date.
 * The stage boundaries will be defined by the number of months from the actual date.
 *
 * As example, if we consider as hot data all data in the las 12 years and today is Sep, 2018, all data with a key starting
 * by 201809 to 201709 will be consider as Hot data an the regions containing that data will be labeled as Hot Regions.
 *
 */
public class StageByDateBuilder implements StageBuilder {

    private static final Log LOG = LogFactory.getLog(StageByDateBuilder.class);

    public static final String NORMALIZER_MONTHS_HOT_KEY_PROPERTY = "hbase.normalizer.nonuniform.months.hot";
    public static final String NORMALIZER_MONTHS_WARM_KEY_PROPERTY = "hbase.normalizer.nonuniform.months.warm";
    public static final String NORMALIZER_MONTHS_COLD_KEY_PROPERTY = "hbase.normalizer.nonuniform.months.cold";


    private static final DateFormat dateFormat = new SimpleDateFormat("yyyyMM");

    /**
     *
     */
    public static final int DEFAULT_HOT_EXPIRATION_IN_MONTHS = 8;

    /**
     *
     */
    public static final int DEFAULT_WARM_EXPIRATION_IN_MONTHS = 18;

    /**
     *
     */
    public static final int DEFAULT_COLD_EXPIRATION_IN_MONTHS = 36;


    /**
     * Length of the hot data interval in months.
     */
    private int hotExpM;

    /**
     * Length of the warm data interval in months.
     */
    private int warmExpM;

    /**
     * Length of the cold data interval in months.
     */
    private int coldExpM;

    private TableName tableName;
    List<HRegionInfo> regionList;


    public List<HRegionInfo> getColdList() {
        return coldList;
    }

    public List<HRegionInfo> getWarmList() {
        return warmList;
    }

    public List<HRegionInfo> getHotList() {
        return hotList;
    }

    List<HRegionInfo> coldList = new ArrayList<>();
    List<HRegionInfo> warmList = new ArrayList<>();
    List<HRegionInfo> hotList = new ArrayList<>();

    //Configuration conf;
    Connection connection;

    /**
     * Default constructor.
     *
     *
     * It will get the initialization properties from the HBase configuration. This
     * includes the number of months used as boundaries hot, warm and cold stages.
     *
     * @param aTableName the name of the table
     * @param aRegionList a list of HRegionInfo
     */
    public StageByDateBuilder(TableName aTableName, List<HRegionInfo> aRegionList) throws IOException {

        this(aTableName, aRegionList, HBaseConfiguration.create());

    }

    /**
     * Default constructor.
     *
     * @param aTableName the name of the table
     * @param aRegionList a list of HRegionInfo
     * @param conf a Configuration instance with the number of months used as boundaries hot, warm and cold stages
     *
     *
     */
    public StageByDateBuilder(TableName aTableName, List<HRegionInfo> aRegionList, Configuration conf) throws IOException {


        connection = ConnectionFactory.createConnection(conf);


        this.tableName = aTableName;
        this.regionList = aRegionList;

        this.hotExpM = conf.getInt(NORMALIZER_MONTHS_HOT_KEY_PROPERTY, DEFAULT_HOT_EXPIRATION_IN_MONTHS);
        this.warmExpM = conf.getInt(NORMALIZER_MONTHS_WARM_KEY_PROPERTY, DEFAULT_WARM_EXPIRATION_IN_MONTHS);
        this.coldExpM = conf.getInt(NORMALIZER_MONTHS_COLD_KEY_PROPERTY, DEFAULT_COLD_EXPIRATION_IN_MONTHS);

    }

    /**
     * Constructor
     *
     * @param aRegionList        name of the table
     * @param hotIntervalLength  Length of the hot data interval in months.
     * @param warmIntervalLength Length of the warm data interval in months.
     * @param coldIntervalLength Length of the cold data interval in months.
     */
    public StageByDateBuilder(TableName aTableName, List<HRegionInfo> aRegionList,
                              int hotIntervalLength, int warmIntervalLength, int coldIntervalLength) throws IOException {

        connection = ConnectionFactory.createConnection(HBaseConfiguration.create());


        this.tableName = aTableName;
        this.regionList = aRegionList;

        this.hotExpM = hotIntervalLength;
        this.warmExpM = warmIntervalLength;
        this.coldExpM = coldIntervalLength;

    }

    public void setStageBoundaries() throws IOException {

        int numRegions = regionList.size();

        LOG.debug("Table " + tableName + " - Total number of regions: " + numRegions);

        RegionLocator locator = connection.getRegionLocator(this.tableName);

        String hotSplitPoint = getHotExpirationSplit();
        HRegionInfo r1 = getRegion(locator, hotSplitPoint);
        LOG.debug("HOT split point:" + hotSplitPoint + ",Region:" + r1.getRegionNameAsString());

        String warmSplitPoint = getWarmExpirationSplit();
        HRegionInfo r2 = getRegion(locator, warmSplitPoint);
        LOG.debug("WARM split point:" + warmSplitPoint + ",Region:" + r2.getRegionNameAsString());

        String coldSplitPoint = getColdExpirationSplit();
        HRegionInfo r3 = getRegion(locator, coldSplitPoint);
        LOG.debug("COLD split point:" + coldSplitPoint + ",Region:" + r3.getRegionNameAsString());


        for (HRegionInfo info : regionList) {
            if (info.compareTo(r2) < 0)
                coldList.add(info);
            else if (info.compareTo(r2) >= 0 && info.compareTo(r1) < 0)
                warmList.add(info);
            else
                hotList.add(info);
        }

        LOG.debug("Table:" + tableName + "-COLD number of regions=" + coldList.size());
        if (LOG.isTraceEnabled()) RegionsUtil.printRegionInfo(coldList);

        LOG.debug("Table:" + tableName + "-WARM number of regions=" + warmList.size());
        if (LOG.isTraceEnabled()) RegionsUtil.printRegionInfo(warmList);

        LOG.debug("Table:" + tableName + "-HOT number of regions=" + hotList.size());
        if (LOG.isTraceEnabled()) RegionsUtil.printRegionInfo(hotList);

        locator.close();

    }


    public HRegionInfo getLastColdRegion() {
        if (this.coldList.size() > 0 )
            return coldList.iterator().next();
        else
            return null;
    }


    /**
     * Returns the list of Regions to archive (older than the cold region split point
     * @return a list of regions.
     * @throws IOException
     */
    public List<HRegionInfo> getRegionsToArchive() throws IOException {

        String coldSplitPoint = getColdExpirationSplit();
        LOG.debug("COLD split point:" + coldSplitPoint);
        return getRegionsToArchive();
    }

    /**
     * Returns the list of Regions older than the region provided as Split point
     * @param sRegionSplitPoint the region name of the las region to keep
     * @return a list of regions older that the region split point
     * @throws IOException
     */
    public List<HRegionInfo> getRegionsToArchive(String sRegionSplitPoint) throws IOException {

        List<HRegionInfo> toDelete = new ArrayList<>();

        RegionLocator locator = connection.getRegionLocator(this.tableName);

        HRegionInfo lastRegionToKeep = getRegion(locator, sRegionSplitPoint);

        LOG.debug("Split point:" + sRegionSplitPoint + ",Region:" + lastRegionToKeep.getRegionNameAsString());


        for (HRegionInfo info : regionList) {
            if  (info.compareTo(lastRegionToKeep) < 0) {
                toDelete.add(info);
            }
            else
                break;
        }


        return toDelete;
    }

    /**
     * Returns the list of Regions of the previous month to the actual date
     * @return
     * @throws IOException
     */
    public List<HRegionInfo> getLastMonthList() throws IOException {
        return getLastMonthList(getLastMonthSplit());
    }


    /**
     * Returns the list of Regions of the previous month to the date provided
     * @param splitDate
     * @return
     * @throws IOException
     */
    public List<HRegionInfo> getLastMonthList(String splitDate) throws IOException {

        List<HRegionInfo> lm = new ArrayList<>();
        RegionLocator locator = connection.getRegionLocator(this.tableName);


        HRegionInfo r1 = getRegion(locator, splitDate);

        for (HRegionInfo info : hotList) {
            if (info.compareTo(r1) >= 0) {
                lm.add(info);
            }

        }
        return lm;

    }


    public static  HRegionInfo getRegion(RegionLocator aLocator, String splitPoint) throws IOException {

        HRegionLocation location =
                aLocator.getRegionLocation(Bytes.toBytes(splitPoint),false);
        return location.getRegionInfo();

    }

    public static  HRegionInfo getRegion(RegionLocator aLocator, String splitPoint, boolean reload) throws IOException {

        HRegionLocation location =
                aLocator.getRegionLocation(Bytes.toBytes(splitPoint),reload);
        return location.getRegionInfo();

    }



    public static String getCurrent() {
        Calendar cal = Calendar.getInstance();
        return dateFormat.format(cal.getTime());

    }

    public static String getPreviousMonth(String splitDate) {

        try {
            Date date = dateFormat.parse(splitDate);
            Calendar cal = Calendar.getInstance();
            cal.setTime(date);
            cal.add(Calendar.MONTH, -1);
            return dateFormat.format(cal.getTime());
        } catch (ParseException e) {
            e.printStackTrace();
        }


        Calendar cal = Calendar.getInstance();
        cal.add(Calendar.MONTH, -1);
        return dateFormat.format(cal.getTime());

    }

    public String getLastMonthSplit() {
        Calendar cal = Calendar.getInstance();
        cal.add(Calendar.MONTH, -1);
        return dateFormat.format(cal.getTime());

    }

    public String getHotExpirationSplit() {
        Calendar cal = Calendar.getInstance();
        cal.add(Calendar.MONTH, -this.hotExpM);
        return dateFormat.format(cal.getTime());

    }

    public String getWarmExpirationSplit() {
        Calendar cal = Calendar.getInstance();
        cal.add(Calendar.MONTH, -this.hotExpM - this.warmExpM);
        return dateFormat.format(cal.getTime());
    }

    public String getColdExpirationSplit() {
        Calendar cal = Calendar.getInstance();
        cal.add(Calendar.MONTH, -this.hotExpM - this.warmExpM - this.coldExpM);
        return dateFormat.format(cal.getTime());

    }

}
