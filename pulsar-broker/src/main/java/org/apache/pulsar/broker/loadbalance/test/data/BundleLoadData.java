package org.apache.pulsar.broker.loadbalance.test.data;

import lombok.Data;
import org.apache.pulsar.policies.data.loadbalancer.TimeAverageMessageData;

@Data
public class BundleLoadData {

    // Short term data for this bundle. The time frame of this data is
    // determined by the number of short term samples
    // and the bundle update period.
    private TimeAverageMessageData shortTermData;

    // Long term data for this bundle. The time frame of this data is determined
    // by the number of long term samples
    // and the bundle update period.
    private TimeAverageMessageData longTermData;

    // number of topics present under this bundle
    private int topics;
}
