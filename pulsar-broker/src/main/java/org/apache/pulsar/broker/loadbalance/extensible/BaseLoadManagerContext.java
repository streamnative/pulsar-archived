package org.apache.pulsar.broker.loadbalance.extensible;

import org.apache.pulsar.broker.loadbalance.extensible.data.BrokerLoadData;
import org.apache.pulsar.broker.loadbalance.extensible.data.BundleLoadData;
import org.apache.pulsar.broker.loadbalance.extensible.data.LoadDataStore;
import org.apache.pulsar.broker.loadbalance.extensible.data.TimeAverageBrokerLoadData;

/**
 * The filter and load balance context, use for delivering context between filter, scheduler and strategy.
 */
public interface BaseLoadManagerContext extends LoadManagerContext {

    LoadDataStore<BrokerLoadData> brokerLoadDataStore();

    LoadDataStore<BundleLoadData> bundleLoadDataStore();

    LoadDataStore<TimeAverageBrokerLoadData> timeAverageBrokerLoadDataStore();

    BrokerRegistry brokerRegistry();
}
