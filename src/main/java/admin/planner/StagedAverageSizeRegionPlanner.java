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
public class StagedAverageSizeRegionPlanner extends SimpleAverageSizeRegionPlanner implements StagedPlanner {

    private static final Log LOG = LogFactory.getLog(StagedAverageSizeRegionPlanner.class);

    public StagedAverageSizeRegionPlanner(Connection connection, TableName tableName){

        this(connection, tableName, HBaseConfiguration.create());

    }

    public StagedAverageSizeRegionPlanner(Connection connection, TableName tableName, Configuration conf){

        super(connection,tableName,conf);

    }


    @Override
    protected List<NormalizationPlan> getPlansForHot(TableName table, List<HRegionInfo> tableRegions) throws IOException {

        LOG.info("Normalizing HOT regions for table " + table);

        int numberOfRegions = tableRegions.size();

        double avgRegionSize = getAverageRegionSize(table, tableRegions);

        LOG.info("Table " + table + "-Regions:" + numberOfRegions
                + "-AverageSize:" + avgRegionSize);

        return getPlansByAverage(table, tableRegions, avgRegionSize);
    }

    @Override
    protected List<NormalizationPlan> getPlansForWarm(TableName table, List<HRegionInfo> tableRegions) throws IOException {

        LOG.info("Normalizing WARM regions for table " + table);

        int numberOfRegions = tableRegions.size();

        double avgRegionSize = getAverageRegionSize(table, tableRegions);

        LOG.info("Table " + table + "-Regions:" + numberOfRegions
                + "-AverageSize:" + avgRegionSize);

        return getPlansByAverage(table, tableRegions, avgRegionSize);

    }

    @Override
    protected List<NormalizationPlan> getPlansForCold(TableName table, List<HRegionInfo> tableRegions) throws IOException {

        LOG.info("Normalizing COLD regions for table " + table);

        int numberOfRegions = tableRegions.size();

        double avgRegionSize = getAverageRegionSize(table, tableRegions);

        LOG.info("Table " + table + "-Regions:" + numberOfRegions
                + "-AverageSize:" + avgRegionSize);

        return getPlansByAverage(table, tableRegions, avgRegionSize);

    }

    public String toString() {
        StringBuffer str = new StringBuffer();
        str.append("Class:" + StagedAverageSizeRegionPlanner.class);
        str.append(":splitFactor:" + splitFactor);
        str.append(super.toString());
        return str.toString();
    }

}
