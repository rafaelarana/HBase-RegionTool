package admin.planner;

import admin.StageBuilder;
import admin.StageByDateBuilder;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.RegionLocator;
import org.apache.hadoop.hbase.master.normalizer.NormalizationPlan;
import org.apache.hadoop.hbase.master.normalizer.SplitNormalizationPlan;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.*;

/**
 * Created by rarana on 18/08/2017.
 */
@InterfaceAudience.Private
public abstract class AbstractRegionPlanner implements NormalizationPlanner {

    private static final Log LOG = LogFactory.getLog(AbstractRegionPlanner.class);

    Map<byte[], RegionLoad> regionLoadMap = Maps.newTreeMap(Bytes.BYTES_COMPARATOR);

    TableName table;
    Connection connection;
    Configuration configuration;
    boolean skipHot = false;
    boolean skipWarm = false;
    boolean skipCold = false;

    public AbstractRegionPlanner(Connection connection, TableName tableName){

        this(connection,tableName,HBaseConfiguration.create());
    }

    public AbstractRegionPlanner(Connection connection, TableName tableName, Configuration conf){
        this.table = tableName;
        this.connection = connection;
        this.configuration = conf;
        skipCold = conf.getBoolean(StagedPlanner.SKIP_COLD_KEY_PROPERTY,false);
        skipWarm = conf.getBoolean(StagedPlanner.SKIP_WARM_KEY_PROPERTY,false);
        skipHot = conf.getBoolean(StagedPlanner.SKIP_HOT_KEY_PROPERTY,false);

    }


    /**
     * Computes next most "urgent" normalization action on the table.
     * Action may be either a split, or a merge, or no action.
     *
     * @param table table to normalize
     * @return a list with the normalization plans to execute
     */
    public List<NormalizationPlan> computePlanForTable(TableName table) throws IOException {

        LOG.info("Normalizing table: " + table);

        if (table == null || table.isSystemTable()) {
            LOG.debug("Normalization of system table " + table + " isn't allowed");
            return null;
        }


        List<HRegionInfo> tableRegions = this.connection.getAdmin().getTableRegions(table);


        try {
            setRegionLoads(table);
        } catch (IOException e) {
            LOG.error("Error initializing RegionLoadPlanner");
            LOG.error(e.getStackTrace().toString());

        }

        List<NormalizationPlan> plans = new ArrayList<>();

        if ( this instanceof StagedPlanner) {

            StageBuilder stageBuilder = new StageByDateBuilder(table, tableRegions, configuration);
            try {
                stageBuilder.setStageBoundaries();
            } catch (IOException e) {
                LOG.error("Cannot parse splits for table " + table + " Cause:" + e.getCause());
                LOG.error(e.getStackTrace());
                return null;
            }


            if (!skipHot) {
                List<HRegionInfo> hotTableRegions = stageBuilder.getHotList();
                plans.addAll(getPlansForHot(table, hotTableRegions));
            }

            if (!skipWarm) {
                List<HRegionInfo> warmTableRegions = stageBuilder.getWarmList();
                plans.addAll(getPlansForWarm(table, warmTableRegions));
            }

            if (!skipCold) {
                List<HRegionInfo> coldTableRegions = stageBuilder.getColdList();
                plans.addAll(getPlansForCold(table, coldTableRegions));
            }


        } else  {
            plans.addAll(getPlans(table, tableRegions));

        }

        if (plans.isEmpty()) {
            LOG.info("No normalization needed, regions look good for table: " + table);
            return null;
        }
        Collections.sort(plans, planComparator);
        return plans;
    }

    protected abstract List<NormalizationPlan> getPlans(TableName table, List<HRegionInfo> tableRegions) throws IOException;

    protected  List<NormalizationPlan> getPlansForHot(TableName table, List<HRegionInfo> tableRegions) throws IOException {
        return new ArrayList<>();
    }

    protected  List<NormalizationPlan> getPlansForWarm(TableName table, List<HRegionInfo> tableRegions) throws IOException  {
        return new ArrayList<>();
    }

    protected  List<NormalizationPlan> getPlansForCold(TableName table, List<HRegionInfo> tableRegions) throws IOException  {
        return new ArrayList<>();
    }


    protected long getRegionSize(HRegionInfo hri) throws IOException {


        long result = 0L;
        RegionLoad regionLoad = this.regionLoadMap.get(hri.getRegionName());

        if(  regionLoad !=  null ) {
            result = regionLoad.getStorefileSizeMB();
        } else {
            LOG.error("No region load for region " + hri.getRegionNameAsString());
        }

        return result;

    }


    protected double getAverageRegionSize(TableName table, List<HRegionInfo> tableRegions) throws IOException {

        long totalSizeMb = 0;

        for (HRegionInfo hri : tableRegions) {
            long regionSize = getRegionSize(hri);
            totalSizeMb += regionSize;
        }

        double avgRegionSize = totalSizeMb / (double) tableRegions.size();

        LOG.info("Table " + table + ", total aggregated regions size: " + totalSizeMb);
        LOG.debug("Table " + table + ", average region size: " + avgRegionSize);

        return avgRegionSize;

    }

    private void setRegionLoads (TableName table) throws IOException {


        Admin admin = this.connection.getAdmin();

        // GEt Regions for table
        RegionLocator regionLocator = connection.getRegionLocator(table);

        Set<ServerName> tableServers = getRegionServersOfTable(regionLocator);


        ClusterStatus clusterStatus = admin.getClusterStatus();


        List<HRegionInfo> tableRegionInfos = admin.getTableRegions(table);



        if (tableRegionInfos == null || tableRegionInfos.isEmpty() ) {
            LOG.warn("NO regions found for table " + table.getNameAsString());
            return;
        }


        Set<byte[]> tableRegionNames = new TreeSet<>(Bytes.BYTES_COMPARATOR);

        for (HRegionInfo regionInfo : tableRegionInfos) {
            if (regionInfo.isOffline() ) {
                LOG.debug("Ignoring region OFFLINE:" + regionInfo.getRegionNameAsString());
            } else {
                tableRegionNames.add(regionInfo.getRegionName());
                LOG.debug("Adding region:" + regionInfo.getRegionNameAsString());
            }
        }

        LOG.debug("Found [" + tableRegionNames.size() + "] regions for table ");

        for (ServerName serverName : tableServers) {

            Map<byte[], RegionLoad> regionsLoadPerServer = clusterStatus.getLoad(serverName).getRegionsLoad();

            for ( byte[] name : tableRegionNames) {
                if (regionsLoadPerServer.containsKey(name)) {
                    regionLoadMap.put(name,regionsLoadPerServer.get(name));
                }
            }

        }

        LOG.debug("Region LOADS SIZE:" + regionLoadMap.values().size() );

        admin.close();

    }

    private Set<ServerName> getRegionServersOfTable(RegionLocator regionLocator)
            throws IOException {

        Set<ServerName> tableServers = Sets.newHashSet();
        for (HRegionLocation regionLocation : regionLocator.getAllRegionLocations()) {
            tableServers.add(regionLocation.getServerName());
        }
        return tableServers;
    }


    public String toString() {
        StringBuffer str = new StringBuffer();
        str.append(":skipHot:" + skipHot);
        str.append(":skipWarm:" + skipWarm);
        str.append(":skipCold:" + skipCold);
        return str.toString();
    }


    // Comparator that gives higher priority to region Split plan
    private Comparator<NormalizationPlan> planComparator =
            new Comparator<NormalizationPlan>() {
                @Override
                public int compare(NormalizationPlan plan, NormalizationPlan plan2) {
                    if (plan instanceof SplitNormalizationPlan) {
                        return -1;
                    }
                    if (plan2 instanceof SplitNormalizationPlan) {
                        return 1;
                    }
                    return 0;
                }
            };


}
