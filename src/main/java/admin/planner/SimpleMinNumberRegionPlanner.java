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
public class SimpleMinNumberRegionPlanner  extends AbstractRegionPlanner {

    private static final Log LOG = LogFactory.getLog(SimpleMinNumberRegionPlanner.class);

    /**
     * Factor used to decide when to split a
     */
    public static final String NORMALIZER_MIN_KEY_PROPERTY = "hbase.normalizer.nonuniform.min.number";

    static final int DEFAULT_MIN_REGION = 3;

    int minNumberOfRegions;


    public SimpleMinNumberRegionPlanner(Connection connection, TableName tableName){

        this(connection, tableName, HBaseConfiguration.create());
    }

    public SimpleMinNumberRegionPlanner(Connection connection, TableName tableName, Configuration conf){

        super(connection,tableName,conf);

        this.minNumberOfRegions = configuration.getInt(NORMALIZER_MIN_KEY_PROPERTY, DEFAULT_MIN_REGION);

    }

    protected List<NormalizationPlan> getPlans(TableName table, List<HRegionInfo> tableRegions) throws IOException {

        LOG.info("Normalizing regions for table " + table);

        int numberOfRegions = tableRegions.size();

        double avgRegionSize = getAverageRegionSize(table, tableRegions);

        LOG.info("Table " + table + " - Regions:" + numberOfRegions
                + " - Min:" + minNumberOfRegions
                + " - AverageSize:" + avgRegionSize);


        List<NormalizationPlan> plans = new ArrayList<>();

        if (numberOfRegions < minNumberOfRegions) {

            LOG.debug("Table " + table + " number of regions (" + numberOfRegions + ") under min ("
                    + minNumberOfRegions + "). Checking for regions  with size over avg to split");

            plans.addAll(getPlansForMinBound(table, tableRegions, avgRegionSize, minNumberOfRegions));

        }

        return plans;
    }


    protected List<NormalizationPlan> getPlansForMinBound(TableName table, List<HRegionInfo> tableRegions, double avgRegionSize, int minNumberOfRegions) throws IOException{


        int numberOfRegions = tableRegions.size();


        List<NormalizationPlan> plans = new ArrayList<>();
        int candidateIdx = 0;
        int counter = minNumberOfRegions - numberOfRegions;
        while (candidateIdx < numberOfRegions) {
            HRegionInfo hri = tableRegions.get(candidateIdx);
            long regionSize = getRegionSize(hri);
            if (regionSize > avgRegionSize || numberOfRegions == 1) {
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


    public String toString() {
        StringBuffer str = new StringBuffer();
        str.append(":Class:" + SimpleMinNumberRegionPlanner.class);
        str.append(":minNumberOfRegions:" + minNumberOfRegions);
        str.append(super.toString());
        return str.toString();
    }


}
