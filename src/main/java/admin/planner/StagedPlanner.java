package admin.planner;

/**
 * Created by rarana on 30/08/2017.
 */
public interface StagedPlanner {

    public static final String  SKIP_HOT_KEY_PROPERTY = "hbase.normalizer.skip.hot";
    public static final String  SKIP_WARM_KEY_PROPERTY = "hbase.normalizer.skip.warm";
    public static final String  SKIP_COLD_KEY_PROPERTY = "hbase.normalizer.skip.cold";
}
