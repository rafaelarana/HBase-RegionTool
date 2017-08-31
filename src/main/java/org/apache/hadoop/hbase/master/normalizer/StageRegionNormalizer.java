package org.apache.hadoop.hbase.master.normalizer;

import admin.StageByDateBuilder;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.master.MasterServices;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

/**
 * Created by Rafael Arana - Cloudera on 22/03/2017.
 */


/**
 * Simple implementation of region normalizer.
 * <p/>
 * Logic in use:
 * <p/>
 * <ol>
 * <li> get all regions of a given table
 * <li> get avg size S of each region (by total size of store files reported in RegionLoad)
 * <li> If biggest region is bigger than S * 2, it is kindly requested to split,
 * and normalization stops
 * <li> Otherwise, two smallest region R1 and its smallest neighbor R2 are kindly requested
 * to merge, if R1 + R1 &lt;  S, and normalization stops
 * <li> Otherwise, no action is performed
 * </ol>
 * <p/>
 * Region sizes are coarse and approximate on the order of megabytes. Additionally,
 * "empty" regions (less than 1MB, with the previous note) are not merged away. This
 * is by design to prevent normalization from undoing the pre-splitting of a table.
 */
@InterfaceAudience.Private
public class StageRegionNormalizer implements RegionNormalizer {

    private static final Log LOG = LogFactory.getLog(StageRegionNormalizer.class);

    /**
     * Factor used to decide when to split a
     */
    private static final String NORMALIZER_SPLIT_FACTOR_KEY_PROPERTY = "hbase.normalizer.nonuniform.split.factor";
    private static final String NORMALIZER_HOT_MIN_KEY_PROPERTY = "hbase.normalizer.nonuniform.hot.min.factor";

    private static final String NORMALIZER_WARM_MIN_KEY_PROPERTY = "hbase.normalizer.nonuniform.warm.min.factor";
    private static final String NORMALIZER_COLD_MIN_KEY_PROPERTY = "hbase.normalizer.nonuniform.cold.min.factor";
    private static final String COLD_MAX_SIZE_IN_MB_KEY_PROPERTY = "hbase.normalizer.nonuniform.cold.max.size";
    private static final String WARM_MAX_SIZE_IN_MB_KEY_PROPERTY = "hbase.normalizer.nonuniform.warm.max.size";
    private static final String HOT_MAX_SIZE_IN_MB_KEY_PROPERTY = "hbase.normalizer.nonuniform.hot.max.size";


    /**
     * Default value for Factor used to compare region size against average size
     */
    private static final int DEFAULT_SPLIT_FACTOR = 2;

    private static final int MIN_REGION_COUNT = 3;

    private static final double DEFAULT_MIN_REGION_FACTOR = 1.;

    private static final long DEFAULT_COLD_MAX_SIZE_IN_MB = 20 * 1000L;
    private static final long DEFAULT_WARM_MAX_SIZE_IN_MB = 10 * 1000L;
    private static final long DEFAULT_HOT_MAX_SIZE_IN_MB = 5 * 1000L;

    private MasterServices masterServices;

    /**
     *  Factor used to compare region size against average size
     */
    private int splitFactor;

    private double coldMinRegionsFactor;
    private double warmMinRegionsFactor;
    private double hotMinRegionsFactor;
    private long coldMaxRegionSz;
    private long warmMaxRegionSz;
    private long hotMaxRegionSz;

    /**
     * Set the master service.
     *
     * @param masterServices inject instance of MasterServices
     */
    @Override
    public void setMasterServices(MasterServices masterServices) {

        this.masterServices = masterServices;
        Configuration configuration = masterServices.getConfiguration();
        this.splitFactor = configuration.getInt(NORMALIZER_SPLIT_FACTOR_KEY_PROPERTY, DEFAULT_SPLIT_FACTOR);

        this.coldMinRegionsFactor = configuration.getDouble(NORMALIZER_COLD_MIN_KEY_PROPERTY, DEFAULT_MIN_REGION_FACTOR);
        this.warmMinRegionsFactor = configuration.getDouble(NORMALIZER_WARM_MIN_KEY_PROPERTY, DEFAULT_MIN_REGION_FACTOR);
        this.hotMinRegionsFactor = configuration.getDouble(NORMALIZER_HOT_MIN_KEY_PROPERTY, DEFAULT_MIN_REGION_FACTOR);

        this.coldMaxRegionSz = configuration.getLong(COLD_MAX_SIZE_IN_MB_KEY_PROPERTY, DEFAULT_COLD_MAX_SIZE_IN_MB);
        this.warmMaxRegionSz = configuration.getLong(WARM_MAX_SIZE_IN_MB_KEY_PROPERTY, DEFAULT_WARM_MAX_SIZE_IN_MB);
        this.hotMaxRegionSz = configuration.getLong(HOT_MAX_SIZE_IN_MB_KEY_PROPERTY, DEFAULT_HOT_MAX_SIZE_IN_MB);


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

    /**
     * Computes next most "urgent" normalization action on the table.
     * Action may be either a split, or a merge, or no action.
     *
     * @param table table to normalize
     * @return normalization plan to execute
     */
    @Override
    public List<NormalizationPlan> computePlanForTable(TableName table) throws HBaseIOException {

        LOG.info("Normalizing table: " + table);

        if (table == null || table.isSystemTable()) {
            LOG.debug("Normalization of system table " + table + " isn't allowed");
            return null;
        }

        List<HRegionInfo> tableRegions = masterServices.getAssignmentManager().getRegionStates().
                getRegionsOfTable(table);


        if (tableRegions == null || tableRegions.size() < MIN_REGION_COUNT) {
            int nrRegions = tableRegions == null ? 0 : tableRegions.size();
            LOG.debug("Table " + table + " has " + nrRegions + " regions, required min number"
                    + " of regions for normalizer to run is " + MIN_REGION_COUNT + ", not running normalizer");
            return null;
        }

        // Get the number of region Servers (only will use the online)
        int numberOfRS = this.masterServices.getServerManager().getOnlineServersList().size();


        StageByDateBuilder rsp = null;
        try {
            rsp = new StageByDateBuilder(table,tableRegions);
            rsp.setStageBoundaries();
        } catch (IOException e) {
            LOG.error("Cannot parse splits for table " + table + " Cause:" + e.getCause());
            LOG.error(e.getStackTrace());
            return null;
        }

        List<HRegionInfo> hotTableRegions = rsp.getHotList();
        List<HRegionInfo> warmTableRegions = rsp.getWarmList();
        List<HRegionInfo> coldTableRegions = rsp.getColdList();


        List<NormalizationPlan> plans = new ArrayList<>();

        plans.addAll(getPlansForHot(table,hotTableRegions, numberOfRS));
        plans.addAll(getPlansForWarm(table,warmTableRegions,numberOfRS));
        plans.addAll(getPlansForCold(table,coldTableRegions,numberOfRS));

        if (plans.isEmpty()) {
            LOG.info("No normalization needed, regions look good for table: " + table);
            return null;
        }
        Collections.sort(plans, planComparator);
        return plans;
    }

    private List<NormalizationPlan> getPlansForHot(TableName table, List<HRegionInfo> tableRegions, int numberOfRS) {

        LOG.info("Normalizing HOT regions for table " + table);

        int numberOfRegions = tableRegions.size();
        //int maxNumberOfRegions = (int)(hotMaxRegionsFactor * numberOfRS);
        int minNumberOfRegions = (int)(hotMinRegionsFactor * numberOfRS);

        double avgRegionSize = getAverageRegionSize(table, tableRegions);

        LOG.info("Table " + table + "-Regions:" + numberOfRegions
                + "-Min:" + minNumberOfRegions
                + "-AverageSize:" + avgRegionSize);

        return getPlans( table, tableRegions,  avgRegionSize,  minNumberOfRegions,hotMaxRegionSz);
    }

    private List<NormalizationPlan> getPlansForWarm(TableName table, List<HRegionInfo> tableRegions,int numberOfRS) {

        LOG.info("Normalizing WARM regions for table " + table);

        int numberOfRegions = tableRegions.size();
        //int maxNumberOfRegions = (int) (warmMaxRegionsFactor * numberOfRS);
        int minNumberOfRegions = (int) (warmMinRegionsFactor * numberOfRS);

        double avgRegionSize = getAverageRegionSize(table, tableRegions);

        LOG.info("Table " + table + "-Regions:" + numberOfRegions
                + "-Min:" + minNumberOfRegions
                + "-AverageSize:" + avgRegionSize);

        return getPlans( table, tableRegions,  avgRegionSize,  minNumberOfRegions,warmMaxRegionSz);

    }

    private List<NormalizationPlan> getPlansForCold(TableName table, List<HRegionInfo> tableRegions,int numberOfRS) {

        LOG.info("Normalizing COLD regions for table " + table);

        int numberOfRegions = tableRegions.size();
        int minNumberOfRegions = (int) (coldMinRegionsFactor * numberOfRS);

        double avgRegionSize = getAverageRegionSize(table, tableRegions);

        LOG.info("Table " + table + "-Regions:" + numberOfRegions
                + "-Min:" + minNumberOfRegions
                + "-AverageSize:" + avgRegionSize);

        return getPlans( table, tableRegions,  avgRegionSize,  minNumberOfRegions, coldMaxRegionSz);

    }

    private List<NormalizationPlan> getPlans(TableName table, List<HRegionInfo> tableRegions, double  avgRegionSize, int minNumberOfRegions, long maxSize) {


        List<NormalizationPlan> plans = new ArrayList<>();

        int numberOfRegions = tableRegions.size();

        if (numberOfRegions < minNumberOfRegions) {

            LOG.debug("Table " + table + " number of regions (" + numberOfRegions + ") under min ("
                    + minNumberOfRegions + "). Checking for regions to split with size over avg");

            plans.addAll(getPlansForMinBound(table, tableRegions, avgRegionSize, minNumberOfRegions));


        /*
        } else if (numberOfRegions > maxNumberOfRegions) {

            LOG.debug("Table " + table + " number of regions (" + numberOfRegions + ") over max ("
                    + maxNumberOfRegions + "). Checking for regions to merge with size under avg");

            plans.addAll(getPlansForMaxBound(table, tableRegions, avgRegionSize,maxNumberOfRegions));

        */
        } else {

            plans.addAll(getPlansForUnbound(table, tableRegions, avgRegionSize,maxSize));


        }

        return plans;
    }


    private List<NormalizationPlan> getPlansForUnbound(TableName table, List<HRegionInfo> tableRegions, double avgRegionSize, long maxSize) {

        List<NormalizationPlan> plans = new ArrayList<>();

        int candidateIdx = 0;
        while (candidateIdx < tableRegions.size()) {
            HRegionInfo hri = tableRegions.get(candidateIdx);
            long regionSize = getRegionSize(hri);
            // if the region is > 2 times larger than average, we split it, split
            // is more high priority normalization action than merge.
            if (regionSize > maxSize) {
                LOG.info("Table " + table + ", large region " + hri.getRegionNameAsString() + " has size "
                        + regionSize + ", more than " + maxSize + " max size, splitting");
                plans.add(new SplitNormalizationPlan(hri, null));
            }
            if (regionSize > splitFactor * avgRegionSize) {
                LOG.info("Table " + table + ", large region " + hri.getRegionNameAsString() + " has size "
                        + regionSize + ", more than " + splitFactor + " avg size, splitting");
                plans.add(new SplitNormalizationPlan(hri, null));
            } else {
                if (candidateIdx == tableRegions.size() - 1) {
                    break;
                }
                HRegionInfo hri2 = tableRegions.get(candidateIdx + 1);
                long regionSize2 = getRegionSize(hri2);
                if (regionSize + regionSize2 < avgRegionSize) {
                    LOG.info("Table " + table + ", small region size: " + regionSize
                            + " plus its neighbor size: " + regionSize2
                            + ", less than the avg size " + avgRegionSize + ", merging them"
                            + hri.getRegionNameAsString() + "," + hri2.getRegionNameAsString());
                    plans.add(new MergeNormalizationPlan(hri, hri2));
                    candidateIdx++;
                }
            }
            candidateIdx++;
        }

        return plans;

    }

    private List<NormalizationPlan> getPlansForMinBound(TableName table, List<HRegionInfo> tableRegions, double avgRegionSize,int minNumberOfRegions) {


        int numberOfRegions = tableRegions.size();

        List<NormalizationPlan> plans = new ArrayList<>();
        int candidateIdx = 0;
        int counter = minNumberOfRegions - numberOfRegions;
        while (candidateIdx < tableRegions.size()) {
            HRegionInfo hri = tableRegions.get(candidateIdx);
            long regionSize = getRegionSize(hri);
            if (regionSize > avgRegionSize) {
                LOG.info("Table " + table + ",  region " + hri.getRegionNameAsString() + " has size "
                        + regionSize + ", more than  avg size, splitting");
                plans.add(new SplitNormalizationPlan(hri, null));
                counter--;
                if (counter == 0) {
                    break;
                }
            }
            candidateIdx++;
        }
        return plans;

    }

    private long getRegionSize(HRegionInfo hri) {
        ServerName sn = masterServices.getAssignmentManager().getRegionStates().
                getRegionServerOfRegion(hri);
        RegionLoad regionLoad = masterServices.getServerManager().getLoad(sn).
                getRegionsLoad().get(hri.getRegionName());
        return regionLoad.getStorefileSizeMB();
    }


    private double getAverageRegionSize(TableName table, List<HRegionInfo> tableRegions) {

        long totalSizeMb = 0;

        for (HRegionInfo hri : tableRegions) {
            long regionSize = getRegionSize(hri);
            totalSizeMb += regionSize;
        }


        double avgRegionSize = totalSizeMb / (double) tableRegions.size();

        LOG.debug("Table " + table + ", total aggregated regions size: " + totalSizeMb);
        LOG.debug("Table " + table + ", average region size: " + avgRegionSize);

        return avgRegionSize;

    }
}

