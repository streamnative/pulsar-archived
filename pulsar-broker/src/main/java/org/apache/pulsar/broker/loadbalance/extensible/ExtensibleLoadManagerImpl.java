/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pulsar.broker.loadbalance.extensible;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.broker.PulsarServerException;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.loadbalance.BrokerFilterException;
import org.apache.pulsar.broker.loadbalance.LeaderBroker;
import org.apache.pulsar.broker.loadbalance.LeaderElectionService;
import org.apache.pulsar.broker.loadbalance.extensible.channel.BundleStateChannel;
import org.apache.pulsar.broker.loadbalance.extensible.data.BrokerLoadData;
import org.apache.pulsar.broker.loadbalance.extensible.data.LoadDataStore;
import org.apache.pulsar.broker.loadbalance.extensible.data.LoadDataStoreException;
import org.apache.pulsar.broker.loadbalance.extensible.data.LoadDataStoreFactory;
import org.apache.pulsar.broker.loadbalance.extensible.data.TopBundlesLoadData;
import org.apache.pulsar.broker.loadbalance.extensible.filter.BrokerFilter;
import org.apache.pulsar.broker.loadbalance.extensible.filter.BrokerVersionFilter;
import org.apache.pulsar.broker.loadbalance.extensible.filter.LargeTopicCountFilter;
import org.apache.pulsar.broker.loadbalance.extensible.reporter.BrokerLoadDataReporter;
import org.apache.pulsar.broker.loadbalance.extensible.reporter.TopBundleLoadDataReporter;
import org.apache.pulsar.broker.loadbalance.extensible.scheduler.LoadManagerScheduler;
import org.apache.pulsar.broker.loadbalance.extensible.scheduler.NamespaceBundleSplitScheduler;
import org.apache.pulsar.broker.loadbalance.extensible.scheduler.NamespaceUnloadScheduler;
import org.apache.pulsar.broker.loadbalance.extensible.strategy.BrokerSelectionStrategy;
import org.apache.pulsar.broker.loadbalance.extensible.strategy.LeastResourceUsageWithWeight;
import org.apache.pulsar.common.naming.ServiceUnitId;
import org.apache.pulsar.metadata.api.coordination.LeaderElectionState;

/**
 * The broker discovery implementation.
 */
@Slf4j
public class ExtensibleLoadManagerImpl implements BrokerDiscovery {

    private PulsarService pulsar;

    private ServiceConfiguration conf;

    private BrokerRegistry brokerRegistry;

    private BaseLoadManagerContext context;

    private BrokerSelectionStrategy brokerSelectionStrategy;

    private List<BrokerFilter> brokerFilterPipeline;

    /**
     * The load data store.
     */
    private LoadDataStore<BrokerLoadData> brokerLoadDataStore;

    private LoadDataStore<TopBundlesLoadData> topBundleLoadDataStore;

    /**
     * The load data reporter.
     */
    private BrokerLoadDataReporter brokerLoadDataReporter;

    private TopBundleLoadDataReporter topBundleLoadDataReporter;

    /**
     * The load manager schedulers.
     */
    @Getter
    private LoadManagerScheduler namespaceUnloadScheduler;

    private LoadManagerScheduler namespaceBundleSplitScheduler;

    private final AtomicBoolean started = new AtomicBoolean(false);

    private LeaderElectionService bundleStateChannelLeaderElectionService;

    private BundleStateChannel bundleStateChannel;

    public ExtensibleLoadManagerImpl() {}


    @Override
    public void start() throws PulsarServerException {
        brokerRegistry = new BrokerRegistryImpl(pulsar);
        // Start the broker registry.
        brokerRegistry.start();
        // Register self to metadata store.
        brokerRegistry.register();
        startBundleStateChannelLeaderElectionService();
        this.bundleStateChannel = new BundleStateChannel(pulsar);

        // Start the load data store.
        try {
            brokerLoadDataStore =
                    LoadDataStoreFactory.create(pulsar, BrokerLoadData.TOPIC, BrokerLoadData.class);
            topBundleLoadDataStore =
                    LoadDataStoreFactory.create(pulsar, TopBundlesLoadData.TOPIC, TopBundlesLoadData.class);
        } catch (LoadDataStoreException e) {
            throw new PulsarServerException(e);
        }


        // Init the load manager context.
        this.context = new BaseLoadManagerContextImpl();
        ((BaseLoadManagerContextImpl) this.context).setConfiguration(this.conf);
        ((BaseLoadManagerContextImpl) this.context).setBrokerRegistry(brokerRegistry);
        ((BaseLoadManagerContextImpl) this.context).setBrokerLoadDataStore(brokerLoadDataStore);
        ((BaseLoadManagerContextImpl) this.context).setTopBundleLoadDataStore(topBundleLoadDataStore);

        this.brokerLoadDataReporter =
                new BrokerLoadDataReporter(pulsar, brokerRegistry.getLookupServiceAddress(), brokerLoadDataStore);

        this.topBundleLoadDataReporter =
                new TopBundleLoadDataReporter(pulsar, brokerRegistry.getLookupServiceAddress(), topBundleLoadDataStore);

        this.pulsar.getLoadManagerExecutor()
                .schedule(() -> brokerLoadDataReporter.reportAsync(false), 500, TimeUnit.MILLISECONDS);
        this.pulsar.getLoadManagerExecutor()
                .schedule(() -> topBundleLoadDataReporter.reportAsync(false), 500, TimeUnit.MILLISECONDS);


        this.namespaceUnloadScheduler = new NamespaceUnloadScheduler(pulsar, context, bundleStateChannel);
        this.namespaceUnloadScheduler.start();
        this.namespaceBundleSplitScheduler = new NamespaceBundleSplitScheduler(pulsar, bundleStateChannel, context);

        // Listen the broker up or down, so we can split immediately.
        // TODO: Report load data when broker up or down.
        this.brokerRegistry.listen((broker) -> namespaceBundleSplitScheduler.execute());
        // Mark the load manager stated, now we can use load data to select best broker for namespace bundle.
        started.set(true);
    }

    protected void startBundleStateChannelLeaderElectionService() {
        this.bundleStateChannelLeaderElectionService = new LeaderElectionService(
                pulsar.getCoordinationService(), pulsar.getSafeWebServiceAddress(),
                state -> {
                    if (state == LeaderElectionState.Leading) {
                        log.info("This broker was elected as bundleStateChanel leader");

                    } else {
                        if (bundleStateChannelLeaderElectionService != null) {
                            log.info("This broker is a bundleStateChanel follower. "
                                            + "Current bundleStateChanel leader is {}",
                                    bundleStateChannelLeaderElectionService.getCurrentLeader());
                        }
                    }
                });
        bundleStateChannelLeaderElectionService.start();
    }

    @Override
    public void initialize(PulsarService pulsar) {
        this.pulsar = pulsar;
        this.conf = pulsar.getConfiguration();
        // TODO: Add a test strategy.
        this.brokerSelectionStrategy = new LeastResourceUsageWithWeight();

        this.brokerFilterPipeline = new ArrayList<>();

        this.brokerFilterPipeline.add(new BrokerVersionFilter());
        this.brokerFilterPipeline.add(new LargeTopicCountFilter());
    }

    @Override
    public Optional<String> discover(ServiceUnitId bundle) {

        BrokerRegistry brokerRegistry = getBrokerRegistry();
        List<String> availableBrokers = brokerRegistry.getAvailableBrokers();

        // When the system namespace doing the bundle lookup,
        // the load manager might not start yet, so we return a random broker.
        if (!started.get() || !isLeader()) {
            return Optional.of(availableBrokers.get(ThreadLocalRandom.current().nextInt(availableBrokers.size())));
        }

        BaseLoadManagerContext context = this.getContext();

        // Filter out brokers that do not meet the rules.
        List<BrokerFilter> filterPipeline = getBrokerFilterPipeline();
        try {
            for (final BrokerFilter filter : filterPipeline) {
                filter.filter(availableBrokers, context);
            }
        } catch (BrokerFilterException e) {
            availableBrokers = brokerRegistry.getAvailableBrokers();
        }

        if (availableBrokers.isEmpty()) {
            return Optional.empty();
        }

        BrokerSelectionStrategy brokerSelectionStrategy = getBrokerSelectionStrategy();

        return brokerSelectionStrategy.select(availableBrokers, bundle, context);
    }

    private CompletableFuture<Optional<String>> getChannelOwnerBroker(ServiceUnitId topic) {
        Optional<LeaderBroker> leader = bundleStateChannelLeaderElectionService.getCurrentLeader();
        if (leader.isPresent()) {
            String broker = leader.get().getServiceUrl();
            //expecting http://broker-xyz:abcd
            broker = broker.substring(broker.lastIndexOf('/') + 1);
            brokerRegistry.lookup(broker);
            return CompletableFuture.completedFuture(Optional.of(broker));
        } else {
            throw new IllegalStateException(
                    "No leader elected from bundleStateChannelLeaderElectionService for topic:" + topic);
        }
    }

    public CompletableFuture<Boolean> checkOwnershipAsync(ServiceUnitId topic, ServiceUnitId bundleUnit) {
        final String bundle = bundleUnit.toString();
        CompletableFuture<Optional<String>> owner;
        if (isInternalTopic(topic.toString())) {
            owner = getChannelOwnerBroker(topic);
        } else {
            owner = bundleStateChannel.getOwner(bundle);
        }

        if (owner == null) {
            return CompletableFuture.failedFuture(
                    new IllegalStateException(
                            String.format("topic:%s, bundle:%s not owned by broker", topic, bundle)));
        }
        return owner.thenApply(broker -> {
            if (broker.get()
                    .equals(brokerRegistry.getLookupServiceAddress())) {
                return true;
            } else {
                return false;
            }
        });
    }

    @Override
    public CompletableFuture<Optional<BrokerLookupData>> assign(
            Optional<ServiceUnitId> topic, ServiceUnitId bundleUnit) {
        final String bundle = bundleUnit.toString();
        CompletableFuture<Optional<String>> owner;
        if (topic.isPresent() && isInternalTopic(topic.get().toString())) {
            owner = getChannelOwnerBroker(topic.get());
        } else {
            owner = bundleStateChannel.getOwner(bundle);
            if (owner == null) {
                owner = CompletableFuture
                        .supplyAsync(() -> discover(bundleUnit), pulsar.getExecutor())
                        .thenCompose(broker -> {
                            if (broker.isPresent()) {
                                return bundleStateChannel.assignBundle(bundle, broker.get());
                            } else {
                                throw new IllegalStateException(
                                        "Failed to discover(select) a candidate broker for bundle:" + bundle);
                            }
                        });
            }
        }


        return owner.thenApply(broker -> getBrokerRegistry().lookup(broker.get()));
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
    protected BrokerSelectionStrategy getBrokerSelectionStrategy() {
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

    protected BundleStateChannel getBundleStateChannel() {
        return this.bundleStateChannel;
    }


    @Override
    public void stop() throws PulsarServerException {
        if (started.compareAndSet(true, false)) {
            try {
                this.brokerRegistry.close();
            } catch (Exception e) {
                throw new PulsarServerException(e);
            }

            try {
                this.brokerLoadDataStore.close();
            } catch (IOException e) {
                throw new PulsarServerException(e);
            }
            try {
                this.topBundleLoadDataStore.close();
            } catch (IOException e) {
                throw new PulsarServerException(e);
            }
        }
    }

    private boolean isInternalTopic(String topic) {
        return topic.startsWith(BundleStateChannel.TOPIC)
                || topic.startsWith(BrokerLoadData.TOPIC)
                || topic.startsWith(TopBundlesLoadData.TOPIC);
    }

    private boolean isLeader() {
        return pulsar.getLeaderElectionService() != null && pulsar.getLeaderElectionService().isLeader();
    }
}
