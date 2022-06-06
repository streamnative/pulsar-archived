package org.apache.pulsar.broker.loadbalance.extensible;

import org.apache.pulsar.broker.PulsarServerException;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.loadbalance.LoadManager;
import org.apache.pulsar.broker.loadbalance.ResourceUnit;
import org.apache.pulsar.common.naming.ServiceUnitId;
import org.apache.pulsar.common.stats.Metrics;
import org.apache.pulsar.policies.data.loadbalancer.LoadManagerReport;

import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

/**
 * Wrapper class allowing classes of instance BrokerDiscoveryImpl to be compatible with the interface LoadManager.
 */
public class ScalableLoadManagerWrapper implements LoadManager {

    private final BrokerDiscoveryImpl brokerDiscovery;

    public ScalableLoadManagerWrapper(BrokerDiscoveryImpl brokerDiscovery) {
        this.brokerDiscovery = brokerDiscovery;
    }

    @Override
    public void start() throws PulsarServerException {
        brokerDiscovery.start();
    }

    @Override
    public void initialize(PulsarService pulsar) {
        brokerDiscovery.initialize(pulsar);
    }

    @Override
    public boolean isCentralized() {
        return true;
    }

    @Override
    public Optional<ResourceUnit> getLeastLoaded(ServiceUnitId su) throws Exception {
        return brokerDiscovery.discover(su);
    }

    @Override
    public void disableBroker() throws Exception {
        brokerDiscovery.getBrokerRegistry().unregister();
    }

    @Override
    public Set<String> getAvailableBrokers() throws Exception {
        return brokerDiscovery.getBrokerRegistry().getAvailableBrokers();
    }

    @Override
    public CompletableFuture<Set<String>> getAvailableBrokersAsync() {
        return brokerDiscovery.getBrokerRegistry().getAvailableBrokersAsync();
    }

    @Override
    public void stop() throws PulsarServerException {
        brokerDiscovery.stop();
    }

    @Override
    public List<Metrics> getLoadBalancingMetrics() {
        return null;
    }

    @Override
    public LoadManagerReport generateLoadReport() throws Exception {
        return null;
    }

    @Override
    public void doLoadShedding() {
        brokerDiscovery.getNamespaceUnloadScheduler().execute();
    }

    @Override
    public void doNamespaceBundleSplit() {
        // No-op.
    }

    @Override
    @Deprecated
    public void setLoadReportForceUpdateFlag() {
        // No-op.
    }

    @Override
    @Deprecated
    public void writeLoadReportOnZookeeper() throws Exception {
        // No-op, this operation is not useful, the load data reporter will automatically write.
    }

    @Override
    @Deprecated
    public void writeResourceQuotasToZooKeeper() throws Exception {
        // No-op, this operation is not useful, the load data reporter will automatically write.
    }
}
