package admin;

import admin.planner.*;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.master.normalizer.NormalizationPlan;
import org.apache.hadoop.hbase.util.AbstractHBaseTool;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by Rafael Arana - Cloudera on 22/03/2017.
 */


/**
 * Tool to manage regions split and merge from the command line.
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
@InterfaceAudience.Public
public class RegionsTool extends AbstractHBaseTool {

    private static final Log LOG = LogFactory.getLog(RegionsTool.class);


    protected static final String OPT_TABLENAME = "tablename";

    protected static final String OPT_USE_MAX_SIZE= "use_max_size";
    protected static final String OPT_USE_AVG_SIZE= "use_avg_size";
    protected static final String OPT_USE_MIN_NUM= "use_min_num";
    protected static final String OPT_USE_MAX_NUM= "use_max_num";
    protected static final String OPT_NO_STAGE= "no_stage";

    protected static final String OPT_PLAN_ONLY = "report";
    protected static final String OPT_ITERATIONS = "iterations";
    protected static final String OPT_SLEEP = "sleep";
    protected static final String OPT_SPLIT_FACTOR = "split_factor";

    protected static final String OPT_MIN_NUM = "min_num";
    protected static final String OPT_MIN_COLD_NUM = "min_cold_num";
    protected static final String OPT_MIN_WARM_NUM = "min_warm_num";
    protected static final String OPT_MIN_HOT_NUM = "min_hot_num";

    protected static final String OPT_MAX_NUM = "max_num";
    protected static final String OPT_MAX_COLD_NUM = "max_cold_num";
    protected static final String OPT_MAX_WARM_NUM = "max_warm_num";
    protected static final String OPT_MAX_HOT_NUM = "max_hot_num";

    protected static final String OPT_MAX_SIZE = "max_size";
    protected static final String OPT_COLD_MAX_SIZE = "cold_max_size";
    protected static final String OPT_WARM_MAX_SIZE = "warm_max_size";
    protected static final String OPT_HOT_MAX_SIZE = "hot_max_size";

    protected static final String OPT_NUM_HOT_MONTHS = "num_hot_months";
    protected static final String OPT_NUM_WARM_MONTHS = "num_warm_months";
    protected static final String OPT_NUM_COLD_MONTHS = "num_cold_months";

    protected static final int DEFAULT_ITERATIONS = 1;
    protected static final int DEFAULT_SLEEP = 300;

    int iterations = DEFAULT_ITERATIONS;
    int sleep = DEFAULT_SLEEP;

    private Connection connection;

    CommandLine cmd;
    List<NormalizationPlanner> plannerList = new ArrayList<>();

    String sTable = null;
    boolean isPlanOnly = false;
    boolean useMaxSize = true;
    boolean useAvgSize = false;
    boolean useMinNum = false;
    boolean useMaxNum = false;
    boolean isMultiStage = true;


    public static void main(String[] args) throws Throwable {

        new RegionsTool().doStaticMain(args);

    }


    /**
     * The "main function" of the tool
     */
    protected int doWork() throws Exception {


        LOG.debug("Table:" + sTable);
        TableName tableName = TableName.valueOf(sTable);

        int counter = 1;

        LOG.debug("Iterations:"+iterations);

        while (counter <= iterations ){

            if (counter > 1) Thread.sleep(sleep * 1000);

            LOG.info("Starting Iteration:" + counter);

            // Init the tool
            init(tableName);


            for (NormalizationPlanner planner: plannerList ) {


                LOG.info("Starting Planner: " + planner.toString());
                // Get  the NormalizationPlans for each planner
                List<NormalizationPlan> plans = planner.computePlanForTable(tableName);

                // Execute the list of plans
                if (!isPlanOnly ) {
                    normalizeRegions(plans);
                }
                LOG.info("End Planner: " + planner.toString());

                if (plans != null && plans.size()>0)  Thread.sleep(10 * 1000);


            }

            // Compute the plans
            //List<NormalizationPlan> plans = computePlanForTable(tableName);

            LOG.info("End Iteration:" + counter);

            counter++;

        }

        return 0;
    }


    /**
     * Set the configuration
     */
    public void init(TableName tableName) throws IOException {

        // init member variables.
        this.connection = ConnectionFactory.createConnection(conf);

        if (useMaxSize) {
            //addPlanner(new MaxSizeRegionPlanner(connection, tableName, conf));
            if (isMultiStage){
                addPlanner(new StagedMaxSizeRegionPlanner(connection, tableName, conf));
            } else {
                addPlanner(new SimpleMaxSizeRegionPlanner(connection, tableName, conf));
            }
        }

        if (useMinNum) {
            //addPlanner(new MinNumberRegionPlanner(connection, tableName, conf));
            if (isMultiStage){
                addPlanner(new StagedMinNumberRegionPlanner(connection, tableName, conf));
            } else {
                addPlanner(new SimpleMinNumberRegionPlanner(connection, tableName, conf));
            }
        }

        if (useMaxNum) {
            //addPlanner(new MaxNumberRegionPlanner(connection, tableName, conf));
            if (isMultiStage){
                addPlanner(new StagedMaxNumberRegionPlanner(connection, tableName, conf));
            } else {
                addPlanner(new SimpleMaxNumberRegionPlanner(connection, tableName, conf));
            }
        }

        if (useAvgSize) {
            if (isMultiStage){
                addPlanner(new StagedAverageSizeRegionPlanner(connection, tableName, conf));
            } else {
                addPlanner(new SimpleAverageSizeRegionPlanner(connection, tableName, conf));
            }
            //addPlanner(new AverageSizeRegionPlanner(connection, tableName, conf));
        }

    }


    @Override
    protected void processOptions(CommandLine cmd) {

        this.cmd = cmd;
        sTable = cmd.getOptionValue(OPT_TABLENAME);
        isPlanOnly = cmd.hasOption(OPT_PLAN_ONLY);
        if (cmd.hasOption(OPT_ITERATIONS)) {
            this.iterations = parseInt(cmd.getOptionValue(OPT_ITERATIONS),DEFAULT_ITERATIONS);
            this.sleep = parseInt(cmd.getOptionValue(OPT_SLEEP),DEFAULT_SLEEP);

        }

        if (cmd.hasOption(OPT_NO_STAGE)) {
            this.isMultiStage = false;

            if ( cmd.hasOption(OPT_MAX_SIZE) ) {
                conf.set(SimpleMaxSizeRegionPlanner.HMAX_SIZE_IN_MB_KEY_PROPERTY,cmd.getOptionValue(OPT_MAX_SIZE));
            }
        } else {
            // Check for the stage boundaries properties
            if ( cmd.hasOption(OPT_NUM_HOT_MONTHS) ) {
                conf.set(StageByDateBuilder.NORMALIZER_MONTHS_HOT_KEY_PROPERTY,cmd.getOptionValue(OPT_NUM_HOT_MONTHS));
            }

            if ( cmd.hasOption(OPT_NUM_WARM_MONTHS) ) {
                conf.set(StageByDateBuilder.NORMALIZER_MONTHS_WARM_KEY_PROPERTY,cmd.getOptionValue(OPT_NUM_WARM_MONTHS));
            }

            if ( cmd.hasOption(OPT_NUM_COLD_MONTHS) ) {
                conf.set(StageByDateBuilder.NORMALIZER_MONTHS_COLD_KEY_PROPERTY,cmd.getOptionValue(OPT_NUM_COLD_MONTHS));
            }


        }


        if ( cmd.hasOption(OPT_USE_MAX_SIZE) && !Boolean.parseBoolean(cmd.getOptionValue(OPT_USE_MAX_SIZE))){
            useMaxSize = false;
        } else {

            if ( cmd.hasOption(OPT_COLD_MAX_SIZE) ) {
                conf.set(StagedMaxSizeRegionPlanner.COLD_MAX_SIZE_IN_MB_KEY_PROPERTY,cmd.getOptionValue(OPT_COLD_MAX_SIZE));
            }

            if ( cmd.hasOption(OPT_WARM_MAX_SIZE) ) {
                conf.set(StagedMaxSizeRegionPlanner.WARM_MAX_SIZE_IN_MB_KEY_PROPERTY,cmd.getOptionValue(OPT_WARM_MAX_SIZE));
            }

            if ( cmd.hasOption(OPT_HOT_MAX_SIZE) ) {
                conf.set(StagedMaxSizeRegionPlanner.HOT_MAX_SIZE_IN_MB_KEY_PROPERTY,cmd.getOptionValue(OPT_HOT_MAX_SIZE));
            }

        }

        if ( cmd.hasOption(OPT_USE_AVG_SIZE) &&  Boolean.parseBoolean(cmd.getOptionValue(OPT_USE_AVG_SIZE))) {
            useAvgSize = true;
            if ( cmd.hasOption(OPT_SPLIT_FACTOR) ) {
                conf.set(SimpleAverageSizeRegionPlanner.NORMALIZER_SPLIT_FACTOR_KEY_PROPERTY,cmd.getOptionValue(OPT_SPLIT_FACTOR));
            }

        }

        if ( cmd.hasOption(OPT_USE_MIN_NUM) &&  Boolean.parseBoolean(cmd.getOptionValue(OPT_USE_MIN_NUM))) {
            useMinNum = true;

            if (isMultiStage) {

                if (cmd.hasOption(OPT_MIN_COLD_NUM)) {
                    conf.set(StagedMinNumberRegionPlanner.NORMALIZER_COLD_MIN_KEY_PROPERTY, cmd.getOptionValue(OPT_MIN_COLD_NUM));
                }

                if (cmd.hasOption(OPT_MIN_WARM_NUM)) {
                    conf.set(StagedMinNumberRegionPlanner.NORMALIZER_WARM_MIN_KEY_PROPERTY, cmd.getOptionValue(OPT_MIN_WARM_NUM));
                }

                if (cmd.hasOption(OPT_MIN_HOT_NUM)) {
                    conf.set(StagedMinNumberRegionPlanner.NORMALIZER_HOT_MIN_KEY_PROPERTY, cmd.getOptionValue(OPT_MIN_HOT_NUM));
                }
            } else {
                if (cmd.hasOption(OPT_MIN_NUM)) {
                    conf.set(SimpleMinNumberRegionPlanner.NORMALIZER_MIN_KEY_PROPERTY, cmd.getOptionValue(OPT_MIN_NUM));
                }
            }
        }

        if ( cmd.hasOption(OPT_USE_MAX_NUM) &&  Boolean.parseBoolean(cmd.getOptionValue(OPT_USE_MAX_NUM))) {
            useMaxNum = true;

            if (isMultiStage) {

                if (cmd.hasOption(OPT_MAX_COLD_NUM)) {
                    conf.set(StagedMaxNumberRegionPlanner.NORMALIZER_COLD_MAX_KEY_PROPERTY, cmd.getOptionValue(OPT_MAX_COLD_NUM));
                }

                if (cmd.hasOption(OPT_MAX_WARM_NUM)) {
                    conf.set(StagedMaxNumberRegionPlanner.NORMALIZER_WARM_MAX_KEY_PROPERTY, cmd.getOptionValue(OPT_MAX_WARM_NUM));
                }

                if (cmd.hasOption(OPT_MAX_HOT_NUM)) {
                    conf.set(StagedMaxNumberRegionPlanner.NORMALIZER_HOT_MAX_KEY_PROPERTY, cmd.getOptionValue(OPT_MAX_HOT_NUM));
                }
            } else {
                if (cmd.hasOption(OPT_MAX_NUM)) {
                    conf.set(SimpleMaxNumberRegionPlanner.NORMALIZER_MAX_KEY_PROPERTY, cmd.getOptionValue(OPT_MAX_NUM));
                }
            }
        }



    }

    @Override
    protected void addOptions() {

        // Common options
        addRequiredOptWithArg("tablename", "Name of the table to normalize");
        addOptWithArg(OPT_ITERATIONS, "Number of iterations it will run the normalization process (defaults to 1).");
        addOptWithArg(OPT_SLEEP, "Number of seconds to sleep between iterations  (defaults to 300 secs)");
        addOptNoArg(OPT_PLAN_ONLY,"Disables plan execution. Only compute the normalization plans.");
        addOptNoArg(OPT_NO_STAGE,"Use for those tables without sets/stages of regions.");


        // Options for Max Size Region Planner
        addOptWithArg(OPT_USE_MAX_SIZE, "[DEFAULT] Split the regions based on the max size per regions. Use the following args to set the limits in MB: + " +
                OPT_COLD_MAX_SIZE + ":" + OPT_WARM_MAX_SIZE + ":" + OPT_HOT_MAX_SIZE);
        addOptWithArg(OPT_MAX_SIZE, "Max size per region, in MB (defaults to 10 GB).");

        addOptWithArg(OPT_COLD_MAX_SIZE, "Max size per region at cold stage, in MB (defaults to 20 GB).");
        addOptWithArg(OPT_WARM_MAX_SIZE, "Max size per region at warm stage, in MB (defaults to 10 GB).");
        addOptWithArg(OPT_HOT_MAX_SIZE, "Max.size per region at hot stage, in MB (defaults to 5 GB).");

        // Options for Average Size Region Planner
        addOptWithArg(OPT_USE_AVG_SIZE, "Computes the average size per stage. Use "+ OPT_SPLIT_FACTOR + " to customize.");
        addOptWithArg(OPT_SPLIT_FACTOR, "Factor used to split regions with size N times over the average (default=2).");

        // Options for Min Number Region Planner
        addOptWithArg(OPT_USE_MIN_NUM, "Applies a minimum number of regions per stage. Set (" + OPT_MIN_COLD_NUM + ","
                + OPT_MIN_WARM_NUM + "," + OPT_MIN_HOT_NUM + ") customize.");

        addOptWithArg(OPT_MIN_NUM, "Number used to set a min number of regions. Default=3");

        addOptWithArg(OPT_MIN_COLD_NUM, "Number used to set a min number of regions in the cold stage. Default=3");
        addOptWithArg(OPT_MIN_WARM_NUM, "Number used to set a min number of regions in the warm stage. Default=3");
        addOptWithArg(OPT_MIN_HOT_NUM, "Number used to set a min number of regions in the hot stage. Default=3");

        // Options for Min Number Region Planner
        addOptWithArg(OPT_USE_MAX_NUM, "Applies a minimum number of regions per stage. Set (" + OPT_MAX_COLD_NUM + ","
                + OPT_MAX_WARM_NUM + "," + OPT_MAX_HOT_NUM + ") customize.");

        addOptWithArg(OPT_MAX_NUM, "Number used to set a max number of regions. Default=10");

        addOptWithArg(OPT_MAX_COLD_NUM, "Number of months data used to set a max number of regions in the cold stage. Default=10");
        addOptWithArg(OPT_MAX_WARM_NUM, "Number used to set a max number of regions in the warm stage. Default=10");
        addOptWithArg(OPT_MAX_HOT_NUM, "Number used to set a max number of regions in the hot stage. Default=10");

        addOptWithArg(OPT_NUM_HOT_MONTHS, "Number of months to consider data as hot. Only used for staged tables. Default="
                + StageByDateBuilder.DEFAULT_HOT_EXPIRATION_IN_MONTHS);
        addOptWithArg(OPT_NUM_WARM_MONTHS, "Number of months to consider data as warm. Only used for staged tables. Default="
                + StageByDateBuilder.DEFAULT_WARM_EXPIRATION_IN_MONTHS);
        addOptWithArg(OPT_NUM_COLD_MONTHS, "Number of months to consider data as  cold before archive it.  Only used for staged tables. Default="
                + StageByDateBuilder.DEFAULT_COLD_EXPIRATION_IN_MONTHS);

    }

    private void addPlanner(NormalizationPlanner planner) {
        //this.planner = loadPlanner;
        plannerList.add(planner);
    }



    /**
     * Execute all the NormalizationPlans
     *
     * @throws IOException
     */
    public void normalizeRegions(List<NormalizationPlan> plans) throws IOException {


        if (plans == null || plans.isEmpty()) {
            // If Region did not generate any plans, it means the cluster is already balanced.
            LOG.info("No plans to execute. Table is balanced");
        } else {
            LOG.info("Starting region normalization.");

            Admin admin = this.connection.getAdmin();

            LOG.debug("Number of plans to execute:" + plans.size());

            for (NormalizationPlan plan : plans) {
                LOG.debug(plan);
                plan.execute(admin);
            }

            admin.close();

            LOG.info("End of region normalization.");

        }

    }

    @Override
    protected void printUsage() {
        printUsage("java " + getClass().getName() + " <options>", "Options:", "");
    }





    public static int parseInt(String s, int defaultValue) {
        return (int) parseLong(s, defaultValue);
    }

    public static long parseLong(String s, long defaultValue) {

        long l;
        try {
            l = Long.parseLong(s);
        } catch (Exception e) {
            l=defaultValue;
        }
        return l;
    }



}

