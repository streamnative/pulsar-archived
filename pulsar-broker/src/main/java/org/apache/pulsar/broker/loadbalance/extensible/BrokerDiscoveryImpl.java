package org.apache.pulsar.broker.loadbalance.extensible;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import lombok.Getter;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.loadbalance.ResourceUnit;
import org.apache.pulsar.broker.loadbalance.extensible.data.BrokerLoadData;
import org.apache.pulsar.broker.loadbalance.extensible.data.BundleLoadData;
import org.apache.pulsar.broker.loadbalance.extensible.data.LoadDataStore;
import org.apache.pulsar.broker.loadbalance.extensible.data.LoadDataStoreException;
import org.apache.pulsar.broker.loadbalance.extensible.data.LoadDataStoreFactory;
import org.apache.pulsar.broker.loadbalance.extensible.data.TimeAverageBrokerLoadData;
import org.apache.pulsar.broker.loadbalance.extensible.filter.BrokerFilter;
import org.apache.pulsar.broker.loadbalance.extensible.filter.BrokerVersionFilter;
import org.apache.pulsar.broker.loadbalance.extensible.filter.LargeTopicCountFilter;
import org.apache.pulsar.broker.loadbalance.extensible.reporter.BrokerLoadDataReporter;
import org.apache.pulsar.broker.loadbalance.extensible.reporter.BundleLoadDataReporter;
import org.apache.pulsar.broker.loadbalance.extensible.reporter.TimeAverageBrokerLoadDataReporter;
import org.apache.pulsar.broker.loadbalance.extensible.scheduler.LoadManagerScheduler;
import org.apache.pulsar.broker.loadbalance.extensible.scheduler.NamespaceBundleSplitScheduler;
import org.apache.pulsar.broker.loadbalance.extensible.scheduler.NamespaceUnloadScheduler;
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
     * The load data store.
     */
    private LoadDataStore<BrokerLoadData> brokerLoadDataStore;

    private LoadDataStore<BundleLoadData> bundleLoadDataStore;

    private LoadDataStore<TimeAverageBrokerLoadData> timeAverageBrokerLoadDataStore;

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
        brokerSelectionStrategy = new BrokerSelectionStrategyImpl();

        brokerFilterPipeline = new ArrayList<>();

        brokerFilterPipeline.add(new BrokerVersionFilter());
        brokerFilterPipeline.add(new LargeTopicCountFilter());

        brokerRegistry = new BrokerRegistryImpl(pulsar);

        brokerRegistry.start();
        brokerRegistry.register();

        try {
            brokerLoadDataStore = LoadDataStoreFactory.create(pulsar, "/broker-load-data", BrokerLoadData.class);
            bundleLoadDataStore = LoadDataStoreFactory.create(pulsar, "/bundle-load-data", BundleLoadData.class);
            timeAverageBrokerLoadDataStore = LoadDataStoreFactory
                    .create(pulsar, "/time-average-broker-load-data", TimeAverageBrokerLoadData.class);
        } catch (LoadDataStoreException e) {
            throw new RuntimeException(e);
        }

        brokerLoadDataReporter =
                new BrokerLoadDataReporter(brokerLoadDataStore, pulsar, brokerRegistry.getLookupServiceAddress());
        bundleLoadDataReporter = new BundleLoadDataReporter(bundleLoadDataStore);
        timeAverageBrokerLoadDataReporter = new TimeAverageBrokerLoadDataReporter(timeAverageBrokerLoadDataStore);

        this.context = BaseLoadManagerContextImpl
                .builder()
                .brokerLoadDataStore(brokerLoadDataStore)
                .bundleLoadDataStore(bundleLoadDataStore)
                .timeAverageBrokerLoadDataStore(timeAverageBrokerLoadDataStore)
                .brokerRegistry(brokerRegistry)
                .configuration(configuration)
                .build();

        namespaceUnloadScheduler = new NamespaceUnloadScheduler(pulsar, context);
        namespaceBundleSplitScheduler = new NamespaceBundleSplitScheduler(pulsar, context);
    }

    @Override
    public void initialize(PulsarService pulsar) {
        this.pulsar = pulsar;
        this.configuration = pulsar.getConfiguration();
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
