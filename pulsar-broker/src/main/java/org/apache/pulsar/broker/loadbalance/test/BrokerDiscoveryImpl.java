package org.apache.pulsar.broker.loadbalance.test;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import com.google.common.collect.Multimap;
import lombok.Getter;
import org.apache.pulsar.broker.PulsarServerException;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.loadbalance.LoadSheddingStrategy;
import org.apache.pulsar.broker.loadbalance.ResourceUnit;
import org.apache.pulsar.broker.loadbalance.impl.LoadManagerShared;
import org.apache.pulsar.broker.loadbalance.test.filter.BrokerFilter;
import org.apache.pulsar.broker.loadbalance.test.reporter.BrokerLoadDataReporter;
import org.apache.pulsar.broker.loadbalance.test.reporter.BundleLoadDataReporter;
import org.apache.pulsar.broker.loadbalance.test.reporter.TimeAverageBrokerLoadDataReporter;
import org.apache.pulsar.broker.loadbalance.test.scheduler.LoadManagerScheduler;
import org.apache.pulsar.broker.loadbalance.test.scheduler.NamespaceBundleSplitScheduler;
import org.apache.pulsar.broker.loadbalance.test.scheduler.NamespaceUnloadScheduler;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.common.naming.ServiceUnitId;

/**
 * The broker discovery implementation.
 */
public class BrokerDiscoveryImpl implements BrokerDiscovery {

    private PulsarService pulsar;

    private ServiceConfiguration configuration;

    private BrokerRegistry brokerRegistry;

    private BaseLoadManagerContext context;

    private BrokerSelectionStrategy brokerSelectionStrategy;

    private List<BrokerFilter> brokerFilterPipeline;

    /**
     * The load reporters.
     */
    private BrokerLoadDataReporter brokerLoadDataReporter;

    private BundleLoadDataReporter bundleLoadDataReporter;

    private TimeAverageBrokerLoadDataReporter timeAverageBrokerLoadDataReporter;

    /**
     * The load manager schedulers.
     */
    @Getter
    private LoadManagerScheduler namespaceUnloadScheduler;

    private LoadManagerScheduler namespaceBundleSplitScheduler;

    public BrokerDiscoveryImpl() {}

    @Override
    public void start() {
        // TODO: new load balancer
        brokerSelectionStrategy = null;
        // TODO: new broker filter pipeline
        brokerFilterPipeline = null;

        namespaceUnloadScheduler = new NamespaceUnloadScheduler(pulsar, context);
        namespaceBundleSplitScheduler = new NamespaceBundleSplitScheduler(pulsar, context);

        brokerRegistry.start();
        brokerRegistry.register();

    }

    @Override
    public void initialize(PulsarService pulsar) {
        this.pulsar = pulsar;
        this.configuration = pulsar.getConfiguration();
        this.brokerRegistry = new BrokerRegistryImpl(pulsar);

    }

    @Override
    public Optional<ResourceUnit> discover(ServiceUnitId serviceUnit) {

        BrokerRegistry brokerRegistry = getBrokerRegistry();
        Set<String> availableBrokers = brokerRegistry.getAvailableBrokers();

        // Filter out brokers that do not meet the rules.
        List<BrokerFilter> filterPipeline = getBrokerFilterPipeline();
        for (final BrokerFilter filter : filterPipeline) {
            filter.filter(availableBrokers, this.getContext());
        }

        if (availableBrokers.isEmpty()) {
            return Optional.empty();
        }

        BrokerSelectionStrategy brokerSelectionStrategy = getBrokerSelectionStrategy(serviceUnit);

        return brokerSelectionStrategy.select(availableBrokers, this.getContext());
    }

    /**
     * Get the broker registry.
     */
    public BrokerRegistry getBrokerRegistry() {
        return this.brokerRegistry;
    }

    /**
     * Get current load balancer we used.
     */
    protected BrokerSelectionStrategy getBrokerSelectionStrategy(ServiceUnitId serviceUnitId) {
        return this.brokerSelectionStrategy;
    }

    /**
     * Get broker filters.
     */
    protected List<BrokerFilter> getBrokerFilterPipeline() {
        return this.brokerFilterPipeline;
    }

    /**
     * Get the context, used for strategy judgment.
     */
    protected BaseLoadManagerContext getContext() {
        return this.context;
    }

    @Override
    public void stop() {

    }
}
