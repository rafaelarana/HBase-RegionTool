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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by rarana on 18/08/2017.
 */
@InterfaceAudience.Private
public class SimpleMaxNumberRegionPlanner extends AbstractRegionPlanner {

    private static final Log LOG = LogFactory.getLog(SimpleMaxNumberRegionPlanner.class);

    /**
     * Factor used to decide when to split a
     */
    public static final String NORMALIZER_MAX_KEY_PROPERTY = "hbase.normalizer.max.num";

    static final int DEFAULT_MAX_REGION = 100;

    private int maxNumRegions;
    private boolean useAverage = true;


    public SimpleMaxNumberRegionPlanner(Connection connection, TableName tableName) {

        this(connection, tableName, HBaseConfiguration.create());
    }

    public SimpleMaxNumberRegionPlanner(Connection connection, TableName tableName, Configuration conf) {

        super(connection, tableName, conf);

        this.maxNumRegions = configuration.getInt(NORMALIZER_MAX_KEY_PROPERTY, DEFAULT_MAX_REGION);

    }

    protected List<NormalizationPlan> getPlans(TableName table, List<HRegionInfo> tableRegions) throws IOException {

        LOG.info("Normalizing regions for table " + table);

        int numberOfRegions = tableRegions.size();

        double avgRegionSize = getAverageRegionSize(table, tableRegions);

        LOG.info("Table " + table + "-Regions:" + numberOfRegions
                + "-Max:" + maxNumRegions
                + "-AverageSize:" + avgRegionSize);

        return getPlans(table, tableRegions, avgRegionSize, maxNumRegions);
    }

    protected List<NormalizationPlan> getPlans(TableName table, List<HRegionInfo> tableRegions, double avgRegionSize,
                                             int maxNumberOfRegions) throws IOException {


        List<NormalizationPlan> plans = new ArrayList<>();

        int numberOfRegions = tableRegions.size();

        if (numberOfRegions > maxNumberOfRegions) {

            LOG.debug("Table " + table + " number of regions (" + numberOfRegions + ") over max ("
                    + maxNumberOfRegions + "). Checking for regions to merge with size over avg");

            if (useAverage) {
                plans.addAll(getPlansForMaxBound(table, tableRegions, avgRegionSize, maxNumberOfRegions));
            } else {
                plans.addAll(getPlansForMaxBound(table, tableRegions, maxNumberOfRegions));
            }

        }

        return plans;
    }

    private List<NormalizationPlan> getPlansForMaxBound(TableName table, List<HRegionInfo> tableRegions,
                                                        double avgRegionSize, int maxNumberOfRegions) throws IOException {

        int numberOfRegions = tableRegions.size();

        List<NormalizationPlan> plans = new ArrayList<>();
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
            if (regionSize < avgRegionSize &&  regionSize2 < avgRegionSize) {
                LOG.info("Table " + table + ", small region size: " + regionSize
                        + " and its neighbor size: " + regionSize2
                        + ", under  the avg size " + avgRegionSize + ", merging them("
                        + hri.getRegionNameAsString() + "," + hri2.getRegionNameAsString());
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

    private List<NormalizationPlan> getPlansForMaxBound(TableName table, List<HRegionInfo> tableRegions,
            int maxNumberOfRegions) throws IOException {

        int numberOfRegions = tableRegions.size();

        List<NormalizationPlan> plans = new ArrayList<>();
        int candidateIdx = 0;
        int counter = numberOfRegions - maxNumberOfRegions;
        while (candidateIdx < tableRegions.size()) {
            if (candidateIdx == tableRegions.size() - 1) {
                break;
            }
            HRegionInfo hri = tableRegions.get(candidateIdx);
            //long regionSize = getRegionSize(hri);
            HRegionInfo hri2 = tableRegions.get(candidateIdx + 1);
            //long regionSize2 = getRegionSize(hri2);

                LOG.info("Table " + table + " merging regions("
                        + hri.getRegionNameAsString() + "," + hri2.getRegionNameAsString());

                plans.add(new MergeNormalizationPlan(hri, hri2));
                candidateIdx++;
                counter--;
                if (counter == 0) {
                    break;
                }

            candidateIdx++;


        }

        return plans;
    }

    public String toString() {
        StringBuffer str = new StringBuffer();
        str.append("Class:" + SimpleMaxNumberRegionPlanner.class);
        str.append(":maxNumRegions:" + maxNumRegions);
        return str.toString();
    }


}
