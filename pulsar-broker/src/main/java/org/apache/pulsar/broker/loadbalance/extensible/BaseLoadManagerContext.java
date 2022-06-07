package org.apache.pulsar.broker.loadbalance.extensible;

import org.apache.pulsar.broker.loadbalance.extensible.data.BrokerLoadData;
import org.apache.pulsar.broker.loadbalance.extensible.data.LoadDataStore;
import org.apache.pulsar.policies.data.loadbalancer.BundleData;
import org.apache.pulsar.policies.data.loadbalancer.TimeAverageBrokerData;
import java.util.Map;

/**
 * The filter and load balance context, use for delivering context between filter, scheduler and strategy.
 */
public interface BaseLoadManagerContext extends LoadManagerContext {

    LoadDataStore<BrokerLoadData> brokerLoadDataStore();

    LoadDataStore<BundleData> bundleLoadDataStore();

    LoadDataStore<TimeAverageBrokerData> timeAverageBrokerLoadDataStore();

    /**
     * <Broker, <Bundle, BundleLoadData>>
     */
    Map<String, BundleData> preallocatedBundleData(String broker);

    BrokerRegistry brokerRegistry();
}
