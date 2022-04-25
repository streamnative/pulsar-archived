package org.apache.pulsar.broker.loadbalance.test;

import org.apache.pulsar.broker.loadbalance.test.data.BrokerLoadData;
import org.apache.pulsar.broker.loadbalance.test.data.BundleLoadData;
import org.apache.pulsar.broker.loadbalance.test.data.LoadDataStore;
import org.apache.pulsar.broker.loadbalance.test.data.TimeAverageBrokerLoadData;

/**
 * The filter and load balance context, use for delivering context between filter, scheduler and strategy.
 */
public interface BaseLoadManagerContext extends LoadManagerContext {

    LoadDataStore<BrokerLoadData> brokerLoadDataStore();

    LoadDataStore<BundleLoadData> bundleLoadDataStore();

    LoadDataStore<TimeAverageBrokerLoadData> timeAverageBrokerLoadDataStore();

    BrokerRegistry brokerRegistry();
}
