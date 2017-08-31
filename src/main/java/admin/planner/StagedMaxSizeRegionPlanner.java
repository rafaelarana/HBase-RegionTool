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
public class StagedMaxSizeRegionPlanner extends  SimpleMaxSizeRegionPlanner implements StagedPlanner {

    private static final Log LOG = LogFactory.getLog(StagedMaxSizeRegionPlanner.class);


    public static final String COLD_MAX_SIZE_IN_MB_KEY_PROPERTY = "hbase.normalizer.nonuniform.cold.max.size";
    public static final String WARM_MAX_SIZE_IN_MB_KEY_PROPERTY = "hbase.normalizer.nonuniform.warm.max.size";
    public static final String HOT_MAX_SIZE_IN_MB_KEY_PROPERTY = "hbase.normalizer.nonuniform.hot.max.size";

    private static final long DEFAULT_COLD_MAX_SIZE_IN_MB = 20 * 1000L;
    private static final long DEFAULT_WARM_MAX_SIZE_IN_MB = 10 * 1000L;
    private static final long DEFAULT_HOT_MAX_SIZE_IN_MB = 5 * 1000L;

    private long coldMaxRegionSz;
    private long warmMaxRegionSz;
    private long hotMaxRegionSz;

    public StagedMaxSizeRegionPlanner(Connection connection, TableName tableName){

        this(connection,tableName, HBaseConfiguration.create());


    }

    public StagedMaxSizeRegionPlanner(Connection connection, TableName tableName, Configuration conf){

        super(connection,tableName,conf);
        // Set boundary conditions
        this.coldMaxRegionSz = configuration.getLong(COLD_MAX_SIZE_IN_MB_KEY_PROPERTY, DEFAULT_COLD_MAX_SIZE_IN_MB);
        this.warmMaxRegionSz = configuration.getLong(WARM_MAX_SIZE_IN_MB_KEY_PROPERTY, DEFAULT_WARM_MAX_SIZE_IN_MB);
        this.hotMaxRegionSz = configuration.getLong(HOT_MAX_SIZE_IN_MB_KEY_PROPERTY, DEFAULT_HOT_MAX_SIZE_IN_MB);


    }

    @Override
    protected List<NormalizationPlan> getPlansForHot(TableName table, List<HRegionInfo> tableRegions) throws IOException {

        LOG.info("Normalizing HOT regions for table " + table);

        return getPlans(table, tableRegions, hotMaxRegionSz);
    }

    @Override
    protected  List<NormalizationPlan> getPlansForWarm(TableName table, List<HRegionInfo> tableRegions) throws IOException {

        LOG.info("Normalizing WARM regions for table " + table);

        return getPlans(table, tableRegions, warmMaxRegionSz);

    }

    @Override
    protected  List<NormalizationPlan> getPlansForCold(TableName table, List<HRegionInfo> tableRegions) throws IOException {

        LOG.info("Normalizing COLD regions for table " + table);

        return getPlans(table, tableRegions,  coldMaxRegionSz);

    }


    public String toString() {
        StringBuffer str = new StringBuffer();
        str.append("Class:MaxSizeRegionPlanner");
        str.append(":coldMaxRegionSz:" + coldMaxRegionSz);
        str.append(":warmMaxRegionSz:" + warmMaxRegionSz);
        str.append(":hotMaxRegionSz:").append(hotMaxRegionSz);
        return str.toString();
    }
}
