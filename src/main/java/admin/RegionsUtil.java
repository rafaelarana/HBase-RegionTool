package admin;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.master.RegionState;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * Created by rarana on 29/09/2017.
 */
public  final class RegionsUtil {

    private static final Log LOG = LogFactory.getLog(RegionsUtil.class);


    /**
     *
     * @param admin
     * @param waitTime number of milliseconds to sleep  between checks
     * @throws IOException
     */
    public static void  checkInTransition(Admin admin, int waitTime) throws IOException {

        boolean inTransitions = true;

        while (inTransitions) {

            Map<String,RegionState> regMap = admin.getClusterStatus().getRegionsInTransition();

            if (regMap.keySet().isEmpty()) {
                inTransitions = false;
            } else {
                LOG.info("Sleeping " + waitTime + " ms. until no Region Server in transition....");
                try {
                    Thread.sleep(waitTime);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }

            }


        }


    }

    static void printRegionInfo(List<HRegionInfo> infos) {
        for (HRegionInfo info : infos) {
            LOG.debug(" Region: " + info.getRegionNameAsString()
                    + " Start Key:" + Bytes.toString(info.getStartKey())
                    + " End Key: " + Bytes.toString(info.getEndKey()));
        }
    }
}
