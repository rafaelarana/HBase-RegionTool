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
import org.apache.hadoop.hbase.master.normalizer.SplitNormalizationPlan;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by rarana on 18/08/2017.
 */
@InterfaceAudience.Private
public class SimpleMaxSizeRegionPlanner  extends AbstractRegionPlanner {

    private static final Log LOG = LogFactory.getLog(SimpleMaxSizeRegionPlanner.class);

    public static final String HMAX_SIZE_IN_MB_KEY_PROPERTY = "hbase.normalizer.nonuniform.max.size";

    private static final long DEFAULT_MAX_SIZE_IN_MB = 10 * 1000L;

    private long maxRegionSz;


    public SimpleMaxSizeRegionPlanner(Connection connection, TableName tableName){
        this(connection,tableName, HBaseConfiguration.create());

    }

    public SimpleMaxSizeRegionPlanner(Connection connection, TableName tableName, Configuration conf){

        super(connection,tableName,conf);
        this.maxRegionSz = configuration.getLong(HMAX_SIZE_IN_MB_KEY_PROPERTY, DEFAULT_MAX_SIZE_IN_MB);

    }


    @Override
    protected  List<NormalizationPlan> getPlans(TableName table, List<HRegionInfo> tableRegions) throws IOException {

        LOG.info("Normalizing  regions for table " + table);

        return getPlans(table, tableRegions,  maxRegionSz);

    }

    protected List<NormalizationPlan> getPlans(TableName table, List<HRegionInfo> tableRegions, long maxSize) throws IOException{

        List<NormalizationPlan> plans = new ArrayList<>();

        int candidateIdx = 0;
        while (candidateIdx < tableRegions.size()) {
            HRegionInfo hri = tableRegions.get(candidateIdx);
            long regionSize = getRegionSize(hri);

            if (regionSize > maxSize) {
                LOG.info("Table " + table + ", large region " + hri.getRegionNameAsString() + " has size "
                        + regionSize + ", more than " + maxSize + " max size, splitting");
                plans.add(new SplitNormalizationPlan(hri, null));
            }

            candidateIdx++;
        }

        return plans;

    }

    public String toString() {
        StringBuffer str = new StringBuffer();
        str.append("Class:MaxSizeRegionPlanner");
        str.append(":maxRegionSz:" + maxRegionSz);
        return str.toString();
    }
}
