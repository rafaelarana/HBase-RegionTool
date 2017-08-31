package org.apache.hadoop.hbase.master.normalizer;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.master.MasterServices;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

/**
 * Created by rarana on 22/03/2017.
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
public class CustomRegionNormalizer implements RegionNormalizer {

    private static final Log LOG = LogFactory.getLog(CustomRegionNormalizer.class);

    // Factor used to compare region size against average size
    private static final int DEFAULT_SPLIT_FACTOR = 2;

    private static final int MIN_REGION_COUNT = 3;
    private static final int MIN_REGION_FACTOR = 1;
    private static final int MAX_REGION_FACTOR = 3;


    private MasterServices masterServices;

    /**
     * Set the master service.
     *
     * @param masterServices inject instance of MasterServices
     */
    @Override
    public void setMasterServices(MasterServices masterServices) {
        this.masterServices = masterServices;
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
        if (table == null || table.isSystemTable()) {
            LOG.debug("Normalization of system table " + table + " isn't allowed");
            return null;
        }

        List<NormalizationPlan> plans = new ArrayList<NormalizationPlan>();
        List<HRegionInfo> tableRegions = masterServices.getAssignmentManager().getRegionStates().
                getRegionsOfTable(table);

        //TODO: should we make min number of regions a config param?
        if (tableRegions == null || tableRegions.size() < MIN_REGION_COUNT) {
            int nrRegions = tableRegions == null ? 0 : tableRegions.size();
            LOG.debug("Table " + table + " has " + nrRegions + " regions, required min number"
                    + " of regions for normalizer to run is " + MIN_REGION_COUNT + ", not running normalizer");
            return null;
        }

        LOG.debug("Computing normalization plan for table: " + table +
                ", number of regions: " + tableRegions.size());


        int numberOfRS = this.masterServices.getServerManager().getOnlineServersList().size();

        int numberOfRegions = tableRegions.size();
        int maxNumberOfRegions = MAX_REGION_FACTOR * numberOfRS;
        int minNumberOfRegions = MIN_REGION_FACTOR * numberOfRS;

       double avgRegionSize = getAverageRegionSize(table, tableRegions);

        if (numberOfRegions < minNumberOfRegions) {

            LOG.info("Table " + table + " number of regions (" + numberOfRegions + ") under min ("
                    + minNumberOfRegions + "). Checking for regions to split with size over avg");

            plans.addAll(getPlansForMinBound(table, tableRegions, avgRegionSize));

        } else if (numberOfRegions > maxNumberOfRegions) {

            LOG.info("Table " + table + " number of regions (" + numberOfRegions + ") over max ("
                    + maxNumberOfRegions + "). Checking for regions to merge with size under avg");

            plans.addAll(getPlansForMaxBound(table, tableRegions, avgRegionSize));

        } else {
            plans.addAll(getPlansForUnbound(table, tableRegions, avgRegionSize));
        }


        if (plans.isEmpty()) {
            LOG.debug("No normalization needed, regions look good for table: " + table);
            return null;
        }
        Collections.sort(plans, planComparator);
        return plans;
    }

    private List<NormalizationPlan> getPlansForUnbound(TableName table, List<HRegionInfo> tableRegions, double avgRegionSize) {

        List<NormalizationPlan> plans = new ArrayList<>();

        int candidateIdx = 0;
        while (candidateIdx < tableRegions.size()) {
            HRegionInfo hri = tableRegions.get(candidateIdx);
            long regionSize = getRegionSize(hri);
            // if the region is > 2 times larger than average, we split it, split
            // is more high priority normalization action than merge.
            if (regionSize > DEFAULT_SPLIT_FACTOR * avgRegionSize) {
                LOG.info("Table " + table + ", large region " + hri.getRegionNameAsString() + " has size "
                        + regionSize + ", more than twice avg size, splitting");
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
                            + ", less than the avg size " + avgRegionSize + ", merging them");
                    plans.add(new MergeNormalizationPlan(hri, hri2));
                    candidateIdx++;
                }
            }
            candidateIdx++;
        }

        return plans;

    }

    private List<NormalizationPlan> getPlansForMaxBound(TableName table, List<HRegionInfo> tableRegions, double avgRegionSize) {

        int numberOfRS = this.masterServices.getServerManager().getOnlineServersList().size();
        int numberOfRegions = tableRegions.size();
        int maxNumberOfRegions = MAX_REGION_FACTOR * numberOfRS;


        List<NormalizationPlan> plans = new ArrayList<NormalizationPlan>();
        int candidateIdx = 0;
        int counter = numberOfRegions - maxNumberOfRegions;
        while (candidateIdx < tableRegions.size()) {
            if (candidateIdx == tableRegions.size() - 1) {
                break;
            }
            HRegionInfo hri = tableRegions.get(candidateIdx);
            long regionSize = getRegionSize(hri);
            HRegionInfo hri2 = tableRegions.get(candidateIdx + 1);
            long regionSize2 = getRegionSize(hri2);
            if (regionSize + regionSize2 < avgRegionSize) {
                LOG.info("Table " + table + ", small region size: " + regionSize
                        + " plus its neighbor size: " + regionSize2
                        + ", less than the avg size " + avgRegionSize + ", merging them("
                        + hri.getRegionNameAsString() + ","  + hri2.getRegionNameAsString());
                plans.add(new MergeNormalizationPlan(hri, hri2));
                candidateIdx++;
                counter--;
                if (counter == 0) {
                    break;
                }

            }
            candidateIdx++;


        }

        return plans;
    }

    private List<NormalizationPlan> getPlansForMinBound(TableName table, List<HRegionInfo> tableRegions, double avgRegionSize) {

        int numberOfRS = this.masterServices.getServerManager().getOnlineServersList().size();
        int numberOfRegions = tableRegions.size();
        int minNumberOfRegions = MIN_REGION_FACTOR * numberOfRS;

        List<NormalizationPlan> plans = new ArrayList<NormalizationPlan>();
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

        for (int i = 0; i < tableRegions.size(); i++) {
            HRegionInfo hri = tableRegions.get(i);
            long regionSize = getRegionSize(hri);
            totalSizeMb += regionSize;
        }


        double avgRegionSize = totalSizeMb / (double) tableRegions.size();

        LOG.debug("Table " + table + ", total aggregated regions size: " + totalSizeMb);
        LOG.debug("Table " + table + ", average region size: " + avgRegionSize);

        return avgRegionSize;

    }
}

