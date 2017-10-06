package admin.planner;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.master.normalizer.MergeNormalizationPlan;
import org.apache.hadoop.hbase.master.normalizer.NormalizationPlan;
import org.apache.hadoop.hbase.master.normalizer.SplitNormalizationPlan;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by rarana on 18/08/2017.
 */
@InterfaceAudience.Private
public class SimpleAverageSizeRegionPlanner extends AbstractRegionPlanner {

    private static final Log LOG = LogFactory.getLog(SimpleAverageSizeRegionPlanner.class);

    /**
     * Factor used to decide when to split a
     */

    public static final String NORMALIZER_SPLIT_FACTOR_KEY_PROPERTY = "hbase.normalizer.nonuniform.split.factor";

    /**
     * Default value for Factor used to compare region size against average size
     */
    private static final int DEFAULT_SPLIT_FACTOR = 2;


    /**
     * Factor used to compare region size against average size
     */
    double splitFactor;


    public SimpleAverageSizeRegionPlanner(Connection connection, TableName tableName){

        this(connection,tableName, HBaseConfiguration.create());

    }

    public SimpleAverageSizeRegionPlanner(Connection connection, TableName tableName, Configuration conf){

        super(connection,tableName,conf);

        this.splitFactor = configuration.getDouble(NORMALIZER_SPLIT_FACTOR_KEY_PROPERTY,DEFAULT_SPLIT_FACTOR);


    }


    protected List<NormalizationPlan> getPlans(TableName table, List<HRegionInfo> tableRegions) throws IOException {

        LOG.info("Normalizing  regions for table " + table);

        //int numberOfRegions = tableRegions.size();

        double avgRegionSize = getAverageRegionSize(table, tableRegions);

        LOG.info("Table:" + table + " - Number of Regions:" + tableRegions.size()
                + " - AverageSize:" + avgRegionSize);

        return getPlansByAverage(table, tableRegions, avgRegionSize);

    }


    protected List<NormalizationPlan> getPlansByAverage(TableName table, List<HRegionInfo> tableRegions, double avgRegionSize) throws IOException{

        List<NormalizationPlan> plans = new ArrayList<>();

        int candidateIdx = 0;
        while (candidateIdx < tableRegions.size()) {
            HRegionInfo hri = tableRegions.get(candidateIdx);
            long regionSize = getRegionSize(hri);
            // if the region is > 2 times larger than average, we split it, split
            // is more high priority normalization action than merge.

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

    public String toString() {
        StringBuffer str = new StringBuffer();
        str.append("Class:" + SimpleAverageSizeRegionPlanner.class);
        str.append(":splitFactor:" + splitFactor);
        return str.toString();
    }

}
