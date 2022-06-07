package org.apache.pulsar.broker.loadbalance.extensible;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import lombok.Getter;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.loadbalance.extensible.data.BrokerLoadData;
import org.apache.pulsar.broker.loadbalance.extensible.data.LoadDataStore;
import org.apache.pulsar.broker.loadbalance.extensible.data.LoadDataStoreException;
import org.apache.pulsar.broker.loadbalance.extensible.data.LoadDataStoreFactory;
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
import org.apache.pulsar.policies.data.loadbalancer.BundleData;
import org.apache.pulsar.policies.data.loadbalancer.NamespaceBundleStats;
import org.apache.pulsar.policies.data.loadbalancer.TimeAverageBrokerData;

/**
 * The broker discovery implementation.
 */
public class BrokerDiscoveryImpl implements BrokerDiscovery {

    // The number of effective samples to keep for observing long term data.
    public static final int NUM_LONG_SAMPLES = 1000;

    // The number of effective samples to keep for observing short term data.
    public static final int NUM_SHORT_SAMPLES = 10;

    // Default message rate to assume for unseen bundles.
    public static final double DEFAULT_MESSAGE_RATE = 50;

    // Default message throughput to assume for unseen bundles.
    // Note that the default message size is implicitly defined as DEFAULT_MESSAGE_THROUGHPUT / DEFAULT_MESSAGE_RATE.
    public static final double DEFAULT_MESSAGE_THROUGHPUT = 50000;

    // The default bundle stats which are used to initialize historic data.
    // This data is overridden after the bundle receives its first sample.
    private final NamespaceBundleStats defaultStats;

    public static final String BROKER_LOAD_DATA_STORE_NAME = "broker-load-data";

    public static final String BUNDLE_LOAD_DATA_STORE_NAME = "bundle-load-data";

    public static final String TIME_AVERAGE_BROKER_LOAD_DATA = "time-average-broker-load-data";

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

    private LoadDataStore<BundleData> bundleLoadDataStore;

    private LoadDataStore<TimeAverageBrokerData> timeAverageBrokerLoadDataStore;

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

    private final AtomicBoolean started = new AtomicBoolean(false);

    public BrokerDiscoveryImpl() {
        defaultStats = new NamespaceBundleStats();
        // Initialize the default stats to assume for unseen bundles (hard-coded for now).
        defaultStats.msgThroughputIn = DEFAULT_MESSAGE_THROUGHPUT;
        defaultStats.msgThroughputOut = DEFAULT_MESSAGE_THROUGHPUT;
        defaultStats.msgRateIn = DEFAULT_MESSAGE_RATE;
        defaultStats.msgRateOut = DEFAULT_MESSAGE_RATE;
    }

    @Override
    public void start() {
        brokerRegistry = new BrokerRegistryImpl(pulsar);

        brokerRegistry.start();
        brokerRegistry.register();

        try {
            brokerLoadDataStore =
                    LoadDataStoreFactory.create(pulsar, BROKER_LOAD_DATA_STORE_NAME, BrokerLoadData.class);
            bundleLoadDataStore = LoadDataStoreFactory.create(pulsar, BUNDLE_LOAD_DATA_STORE_NAME, BundleData.class);
            timeAverageBrokerLoadDataStore = LoadDataStoreFactory
                    .create(pulsar, TIME_AVERAGE_BROKER_LOAD_DATA, TimeAverageBrokerData.class);
        } catch (LoadDataStoreException e) {
            throw new RuntimeException(e);
        }

        brokerLoadDataReporter =
                new BrokerLoadDataReporter(brokerLoadDataStore, pulsar, brokerRegistry.getLookupServiceAddress());
        bundleLoadDataReporter = new BundleLoadDataReporter(bundleLoadDataStore);
        timeAverageBrokerLoadDataReporter = new TimeAverageBrokerLoadDataReporter(timeAverageBrokerLoadDataStore);

        namespaceUnloadScheduler = new NamespaceUnloadScheduler(pulsar, context);
        namespaceBundleSplitScheduler = new NamespaceBundleSplitScheduler(pulsar, context);
        ((BaseLoadManagerContextImpl) this.context).setBrokerRegistry(brokerRegistry);
        ((BaseLoadManagerContextImpl) this.context).setBrokerLoadDataStore(brokerLoadDataStore);
        ((BaseLoadManagerContextImpl) this.context).setBundleLoadDataStore(bundleLoadDataStore);
        ((BaseLoadManagerContextImpl) this.context).setTimeAverageBrokerLoadDataStore(timeAverageBrokerLoadDataStore);

        started.set(true);
    }

    @Override
    public void initialize(PulsarService pulsar) {
        this.pulsar = pulsar;
        this.configuration = pulsar.getConfiguration();

        this.context = new BaseLoadManagerContextImpl();
        ((BaseLoadManagerContextImpl) this.context).setConfiguration(configuration);


        brokerSelectionStrategy = new LeastLongTermMessageRateStrategyImpl();

        brokerFilterPipeline = new ArrayList<>();

        brokerFilterPipeline.add(new BrokerVersionFilter());
        brokerFilterPipeline.add(new LargeTopicCountFilter());

    }

    @Override
    public Optional<String> discover(ServiceUnitId serviceUnit) {

        final String bundle = serviceUnit.toString();
        BrokerRegistry brokerRegistry = getBrokerRegistry();
        List<String> availableBrokers = brokerRegistry.getAvailableBrokers();

        if (!started.get()) {
            return Optional.of(availableBrokers.get(ThreadLocalRandom.current().nextInt(availableBrokers.size())));
        }

        BaseLoadManagerContext context = this.getContext();

        // Filter out brokers that do not meet the rules.
        List<BrokerFilter> filterPipeline = getBrokerFilterPipeline();
        for (final BrokerFilter filter : filterPipeline) {
            filter.filter(availableBrokers, context);
        }

        if (availableBrokers.isEmpty()) {
            return Optional.empty();
        }

        BrokerSelectionStrategy brokerSelectionStrategy = getBrokerSelectionStrategy(serviceUnit);

        Optional<String> selectedBroker = brokerSelectionStrategy.select(availableBrokers, context);
        BundleData bundleData = bundleLoadDataStore.get(bundle);
        if (bundleData == null) {
            bundleData = new BundleData(NUM_SHORT_SAMPLES, NUM_LONG_SAMPLES, defaultStats);
        }
        context.preallocatedBundleData(selectedBroker.get()).put(bundle, bundleData);
        return selectedBroker;
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
