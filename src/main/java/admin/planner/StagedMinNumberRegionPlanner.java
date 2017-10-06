package admin.planner;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.master.normalizer.NormalizationPlan;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by rarana on 18/08/2017.
 */
@InterfaceAudience.Private
public class StagedMinNumberRegionPlanner extends SimpleMinNumberRegionPlanner implements StagedPlanner {

    private static final Log LOG = LogFactory.getLog(StagedMinNumberRegionPlanner.class);

    /**
     * Factor used to decide when to split a
     */
    public static final String NORMALIZER_HOT_MIN_KEY_PROPERTY = "hbase.normalizer.nonuniform.hot.min.factor";
    public static final String NORMALIZER_WARM_MIN_KEY_PROPERTY = "hbase.normalizer.nonuniform.warm.min.factor";
    public static final String NORMALIZER_COLD_MIN_KEY_PROPERTY = "hbase.normalizer.nonuniform.cold.min.factor";

    private int coldMinRegionsFactor;
    private int warmMinRegionsFactor;
    private int hotMinRegionsFactor;


    public StagedMinNumberRegionPlanner(Connection connection, TableName tableName){

        this(connection, tableName, HBaseConfiguration.create());
    }

    public StagedMinNumberRegionPlanner(Connection connection, TableName tableName, Configuration conf){

        super(connection,tableName,conf);

        this.coldMinRegionsFactor = configuration.getInt(NORMALIZER_COLD_MIN_KEY_PROPERTY, DEFAULT_MIN_REGION);
        this.warmMinRegionsFactor = configuration.getInt(NORMALIZER_WARM_MIN_KEY_PROPERTY, DEFAULT_MIN_REGION);
        this.hotMinRegionsFactor = configuration.getInt(NORMALIZER_HOT_MIN_KEY_PROPERTY, DEFAULT_MIN_REGION);

    }

    protected List<NormalizationPlan> getPlansForHot(TableName table, List<HRegionInfo> tableRegions) throws IOException {

        LOG.info("Normalizing HOT regions for table " + table);

        int numberOfRegions = tableRegions.size();
        int minNumberOfRegions = hotMinRegionsFactor;

        double avgRegionSize = getAverageRegionSize(table, tableRegions);

        LOG.info("Table " + table + "-Regions:" + numberOfRegions
                + "-Min:" + minNumberOfRegions
                + "-AverageSize:" + avgRegionSize);

        return getPlans(table, tableRegions, avgRegionSize, minNumberOfRegions);
    }

    protected List<NormalizationPlan> getPlansForWarm(TableName table, List<HRegionInfo> tableRegions) throws IOException {

        LOG.info("Normalizing WARM regions for table " + table);

        int numberOfRegions = tableRegions.size();
        int minNumberOfRegions = warmMinRegionsFactor;

        double avgRegionSize = getAverageRegionSize(table, tableRegions);

        LOG.info("Table " + table + "-Regions:" + numberOfRegions
                + "-Min:" + minNumberOfRegions
                + "-AverageSize:" + avgRegionSize);

        return getPlans(table, tableRegions, avgRegionSize, minNumberOfRegions);

    }

    protected List<NormalizationPlan> getPlansForCold(TableName table, List<HRegionInfo> tableRegions) throws IOException {

        LOG.info("Normalizing COLD regions for table " + table);

        int numberOfRegions = tableRegions.size();
        int minNumberOfRegions = coldMinRegionsFactor;

        double avgRegionSize = getAverageRegionSize(table, tableRegions);

        LOG.info("Table " + table + "-Regions:" + numberOfRegions
                + "-Min:" + minNumberOfRegions);

        return getPlans(table, tableRegions, avgRegionSize, minNumberOfRegions);

    }


    private List<NormalizationPlan> getPlans(TableName table, List<HRegionInfo> tableRegions, double avgRegionSize, int minNumberOfRegions) throws IOException{


        List<NormalizationPlan> plans = new ArrayList<>();

        int numberOfRegions = tableRegions.size();

        if (numberOfRegions < minNumberOfRegions) {

            LOG.debug("Table " + table + " number of regions (" + numberOfRegions + ") under min ("
                    + minNumberOfRegions + "). Checking for regions to split with size over avg");

            plans.addAll(getPlansForMinBound(table, tableRegions, avgRegionSize, minNumberOfRegions));

        }

        return plans;
    }



    public String toString() {
        StringBuffer str = new StringBuffer();
        str.append("Class:" + StagedMinNumberRegionPlanner.class);
        str.append(":coldMinRegionsNumber:" + coldMinRegionsFactor);
        str.append(":warmMinRegionsNumber:" + warmMinRegionsFactor);
        str.append(":hotMinRegionsNumber:" + hotMinRegionsFactor);
        str.append(super.toString());
        return str.toString();
    }


}
