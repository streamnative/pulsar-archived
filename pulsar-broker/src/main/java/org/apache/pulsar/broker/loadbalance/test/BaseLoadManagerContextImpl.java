package org.apache.pulsar.broker.loadbalance.test;

import lombok.Builder;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.loadbalance.test.data.BrokerLoadData;
import org.apache.pulsar.broker.loadbalance.test.data.BundleLoadData;
import org.apache.pulsar.broker.loadbalance.test.data.LoadDataStore;
import org.apache.pulsar.broker.loadbalance.test.data.TimeAverageBrokerLoadData;

@Builder
public class BaseLoadManagerContextImpl implements BaseLoadManagerContext {

    private LoadDataStore<BrokerLoadData> brokerLoadDataStore;

    private LoadDataStore<BundleLoadData> bundleLoadDataStore;

    private LoadDataStore<TimeAverageBrokerLoadData> timeAverageBrokerLoadDataStore;

    private BrokerRegistry brokerRegistry;

    private ServiceConfiguration configuration;

    @Override
    public LoadDataStore<BrokerLoadData> brokerLoadDataStore() {
        return this.brokerLoadDataStore;
    }

    @Override
    public LoadDataStore<BundleLoadData> bundleLoadDataStore() {
        return this.bundleLoadDataStore;
    }

    @Override
    public LoadDataStore<TimeAverageBrokerLoadData> timeAverageBrokerLoadDataStore() {
        return this.timeAverageBrokerLoadDataStore;
    }

    @Override
    public BrokerRegistry brokerRegistry() {
        return this.brokerRegistry;
    }

    @Override
    public ServiceConfiguration brokerConfiguration() {
        return this.configuration;
    }
}
