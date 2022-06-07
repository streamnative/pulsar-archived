package org.apache.pulsar.broker.loadbalance.extensible;

import lombok.Data;
import lombok.Setter;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.loadbalance.extensible.data.BrokerLoadData;
import org.apache.pulsar.broker.loadbalance.extensible.data.LoadDataStore;
import org.apache.pulsar.policies.data.loadbalancer.BundleData;
import org.apache.pulsar.policies.data.loadbalancer.TimeAverageBrokerData;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Data
@Setter
public class BaseLoadManagerContextImpl implements BaseLoadManagerContext {

    public BaseLoadManagerContextImpl() {
        this.preallocatedBundleData = new ConcurrentHashMap<>();
    }

    private LoadDataStore<BrokerLoadData> brokerLoadDataStore;

    private LoadDataStore<BundleData> bundleLoadDataStore;

    private LoadDataStore<TimeAverageBrokerData> timeAverageBrokerLoadDataStore;

    private Map<String, Map<String, BundleData>> preallocatedBundleData;

    private BrokerRegistry brokerRegistry;

    private ServiceConfiguration configuration;

    @Override
    public LoadDataStore<BrokerLoadData> brokerLoadDataStore() {
        return this.brokerLoadDataStore;
    }

    @Override
    public LoadDataStore<BundleData> bundleLoadDataStore() {
        return this.bundleLoadDataStore;
    }

    @Override
    public LoadDataStore<TimeAverageBrokerData> timeAverageBrokerLoadDataStore() {
        return this.timeAverageBrokerLoadDataStore;
    }

    @Override
    public Map<String, BundleData> preallocatedBundleData(String broker) {
        return this.preallocatedBundleData.computeIfAbsent(broker, (__) -> new ConcurrentHashMap<>());
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
