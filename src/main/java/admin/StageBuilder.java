package admin;

import org.apache.hadoop.hbase.HRegionInfo;

import java.io.IOException;
import java.util.List;

/**
 * Created by rarana on 31/08/2017.
 *
 * Interface use to split the regions in three stages (Hot, Warm and Cold)
 *
 */
public interface StageBuilder {

    /**
     *
     * Builds the stages
     * @throws IOException
     */
    void setStageBoundaries() throws IOException;

    /**
     * Returns the list of regions in the Cold Stage
     * @return List of HRegionInfo
     */
    List<HRegionInfo> getColdList();

    /**
     * Returns the list of regions in the Warm Stage
     * @return List of HRegionInfo
     */
    List<HRegionInfo> getWarmList();

    /**
     * Returns the list of regions in the Hot Stage
     * @return List of HRegionInfo
     */
    List<HRegionInfo> getHotList();

    List<HRegionInfo> getLastMonthList() throws IOException;
    List<HRegionInfo> getLastMonthList(String splitDate) throws IOException;
}
