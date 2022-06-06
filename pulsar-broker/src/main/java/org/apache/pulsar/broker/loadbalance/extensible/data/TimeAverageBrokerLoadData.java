package org.apache.pulsar.broker.loadbalance.extensible.data;

import java.util.Map;
import java.util.Set;
import lombok.Data;
import org.apache.pulsar.policies.data.loadbalancer.BundleData;
import org.apache.pulsar.policies.data.loadbalancer.NamespaceBundleStats;
import org.apache.pulsar.policies.data.loadbalancer.TimeAverageMessageData;

@Data
public class TimeAverageBrokerLoadData {

    private double shortTermMsgThroughputIn;
    private double shortTermMsgThroughputOut;
    private double shortTermMsgRateIn;
    private double shortTermMsgRateOut;
    private double longTermMsgThroughputIn;
    private double longTermMsgThroughputOut;
    private double longTermMsgRateIn;
    private double longTermMsgRateOut;

    /**
     * Initialize a TimeAverageBrokerData.
     *
     * @param bundles
     *            The bundles belonging to the broker.
     * @param data
     *            Map from bundle names to the data for that bundle.
     * @param defaultStats
     *            The stats to use when a bundle belonging to this broker is not found in the bundle data map.
     */
    public TimeAverageBrokerLoadData(final Set<String> bundles, final Map<String, BundleData> data,
                                 final NamespaceBundleStats defaultStats) {
        reset(bundles, data, defaultStats);
    }

    /**
     * Reuse this TimeAverageBrokerData using new data.
     *
     * @param bundles
     *            The bundles belonging to the broker.
     * @param data
     *            Map from bundle names to the data for that bundle.
     * @param defaultStats
     *            The stats to use when a bundle belonging to this broker is not found in the bundle data map.
     */
    public void reset(final Set<String> bundles, final Map<String, BundleData> data,
                      final NamespaceBundleStats defaultStats) {
        shortTermMsgThroughputIn = 0;
        shortTermMsgThroughputOut = 0;
        shortTermMsgRateIn = 0;
        shortTermMsgRateOut = 0;

        longTermMsgThroughputIn = 0;
        longTermMsgThroughputOut = 0;
        longTermMsgRateIn = 0;
        longTermMsgRateOut = 0;

        for (String bundle : bundles) {
            final BundleData bundleData = data.get(bundle);
            if (bundleData == null) {
                shortTermMsgThroughputIn += defaultStats.msgThroughputIn;
                shortTermMsgThroughputOut += defaultStats.msgThroughputOut;
                shortTermMsgRateIn += defaultStats.msgRateIn;
                shortTermMsgRateOut += defaultStats.msgRateOut;

                longTermMsgThroughputIn += defaultStats.msgThroughputIn;
                longTermMsgThroughputOut += defaultStats.msgThroughputOut;
                longTermMsgRateIn += defaultStats.msgRateIn;
                longTermMsgRateOut += defaultStats.msgRateOut;
            } else {
                final TimeAverageMessageData shortTermData = bundleData.getShortTermData();
                final TimeAverageMessageData longTermData = bundleData.getLongTermData();

                shortTermMsgThroughputIn += shortTermData.getMsgThroughputIn();
                shortTermMsgThroughputOut += shortTermData.getMsgThroughputOut();
                shortTermMsgRateIn += shortTermData.getMsgRateIn();
                shortTermMsgRateOut += shortTermData.getMsgRateOut();

                longTermMsgThroughputIn += longTermData.getMsgThroughputIn();
                longTermMsgThroughputOut += longTermData.getMsgThroughputOut();
                longTermMsgRateIn += longTermData.getMsgRateIn();
                longTermMsgRateOut += longTermData.getMsgRateOut();
            }
        }
    }
}
