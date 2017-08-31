package admin.planner;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.master.normalizer.NormalizationPlan;

import java.io.IOException;
import java.util.List;

/**
 * Created by rarana on 28/08/2017.
 */
public interface NormalizationPlanner {

    List<NormalizationPlan> computePlanForTable(TableName table) throws IOException;

    String toString();
}
