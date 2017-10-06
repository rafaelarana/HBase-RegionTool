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
import java.util.List;

/**
 * Created by rarana on 18/08/2017.
 */
@InterfaceAudience.Private
public class StagedMaxNumberRegionPlanner extends SimpleMaxNumberRegionPlanner implements StagedPlanner {

    private static final Log LOG = LogFactory.getLog(StagedMaxNumberRegionPlanner.class);

    /**
     * Factor used to decide when to split a
     */
    public static final String NORMALIZER_HOT_MAX_KEY_PROPERTY = "hbase.normalizer.hot.max.num";
    public static final String NORMALIZER_WARM_MAX_KEY_PROPERTY = "hbase.normalizer.warm.max.num";
    public static final String NORMALIZER_COLD_MAX_KEY_PROPERTY = "hbase.normalizer.cold.max.num";


    private int coldMaxNumRegions;
    private int warmMaxNumRegions;
    private int hotMaxNumRegions;

    public StagedMaxNumberRegionPlanner(Connection connection, TableName tableName) {

        this(connection, tableName, HBaseConfiguration.create());
    }

    public StagedMaxNumberRegionPlanner(Connection connection, TableName tableName, Configuration conf) {

        super(connection, tableName, conf);

        this.coldMaxNumRegions = configuration.getInt(NORMALIZER_COLD_MAX_KEY_PROPERTY, DEFAULT_MAX_REGION);
        this.warmMaxNumRegions = configuration.getInt(NORMALIZER_WARM_MAX_KEY_PROPERTY, DEFAULT_MAX_REGION);
        this.hotMaxNumRegions = configuration.getInt(NORMALIZER_HOT_MAX_KEY_PROPERTY, DEFAULT_MAX_REGION);

    }

    protected List<NormalizationPlan> getPlansForHot(TableName table, List<HRegionInfo> tableRegions) throws IOException {

        LOG.info("Normalizing HOT regions for table " + table);

        int numberOfRegions = tableRegions.size();
        int maxNumberOfRegions = hotMaxNumRegions;

        double avgRegionSize = getAverageRegionSize(table, tableRegions);

        LOG.info("Table " + table + "-Regions:" + numberOfRegions
                + "-Max:" + maxNumberOfRegions
                + "-AverageSize:" + avgRegionSize);

        return getPlans(table, tableRegions, avgRegionSize, maxNumberOfRegions);
    }

    protected List<NormalizationPlan> getPlansForWarm(TableName table, List<HRegionInfo> tableRegions) throws IOException {

        LOG.info("Normalizing WARM regions for table " + table);

        int numberOfRegions = tableRegions.size();
        int maxNumberOfRegions = warmMaxNumRegions;

        double avgRegionSize = getAverageRegionSize(table, tableRegions);

        LOG.info("Table " + table + "-Regions:" + numberOfRegions
                + "-Max:" + maxNumberOfRegions
                + "-AverageSize:" + avgRegionSize);


        return getPlans(table, tableRegions, avgRegionSize, maxNumberOfRegions);

    }

    protected List<NormalizationPlan> getPlansForCold(TableName table, List<HRegionInfo> tableRegions) throws IOException {

        LOG.info("Normalizing COLD regions for table " + table);

        int numberOfRegions = tableRegions.size();
        int maxNumberOfRegions = coldMaxNumRegions;

        double avgRegionSize = getAverageRegionSize(table, tableRegions);

        LOG.info("Table " + table + "-Regions:" + numberOfRegions
                + "-Max:" + maxNumberOfRegions);

        return getPlans(table, tableRegions, avgRegionSize, maxNumberOfRegions);

    }

    public String toString() {
        StringBuffer str = new StringBuffer();
        str.append("Class:" + StagedMaxNumberRegionPlanner.class);
        str.append(":coldMaxNumRegions:" + coldMaxNumRegions);
        str.append(":warmMaxNumRegions:" + warmMaxNumRegions);
        str.append(":hotMaxNumRegions:" + hotMaxNumRegions);
        str.append(super.toString());
        return str.toString();
    }


}
