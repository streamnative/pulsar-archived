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
import org.apache.pulsar.broker.lookup.LookupResult;
import org.apache.pulsar.common.naming.ServiceUnitId;
import org.apache.pulsar.common.util.collections.ConcurrentOpenHashMap;
import org.apache.pulsar.metadata.api.NotificationType;

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

    private BundleStateChannel bundleStateChannel;


    private final ConcurrentOpenHashMap<String, CompletableFuture<Optional<LookupResult>>>
            lookupRequests = ConcurrentOpenHashMap.<String,
                    CompletableFuture<Optional<LookupResult>>>newBuilder()
            .build();

    public ExtensibleLoadManagerImpl() {}


    @Override
    public void start() throws PulsarServerException {
        brokerRegistry = new BrokerRegistryImpl(pulsar);
        // Start the broker registry.
        brokerRegistry.start();
        // Register self to metadata store.
        brokerRegistry.register();
        bundleStateChannel = new BundleStateChannel(pulsar);
        bundleStateChannel.start();

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
        this.brokerRegistry.listen((broker, __) -> namespaceBundleSplitScheduler.execute());
        this.brokerRegistry.listen((broker, type) -> {
                if (type == NotificationType.Deleted) {
                    log.info("BrokerRegistry detected the broker {} is deleted. Cleaning bundle ownerships", broker);
                    bundleStateChannel.cleanBundleOwnerships(broker);
                }
        });

        // Mark the load manager stated, now we can use load data to select best broker for namespace bundle.
        started.set(true);
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



    public CompletableFuture<Boolean> checkOwnershipAsync(ServiceUnitId topic, ServiceUnitId bundleUnit) {
        final String bundle = bundleUnit.toString();
        CompletableFuture<Optional<String>> owner;
        if (isInternalTopic(topic.toString())) {
            owner = bundleStateChannel.getChannelOwnerBroker(topic);
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
    public CompletableFuture<Optional<LookupResult>> assign(
            Optional<ServiceUnitId> topic, ServiceUnitId bundleUnit) {

        final String bundle = bundleUnit.toString();
        return lookupRequests.computeIfAbsent(bundle, k -> {
            CompletableFuture<Optional<LookupResult>> future = new CompletableFuture<>();
            CompletableFuture<Optional<String>> owner;
            if (topic.isPresent() && isInternalTopic(topic.get().toString())) {
                owner = bundleStateChannel.getChannelOwnerBroker(topic.get());
            } else {
                owner = bundleStateChannel.getOwner(bundle);
                if (owner == null) {
                    log.warn("No owner is found. Starting broker assignment for bundle:{}", bundle);
                    owner = CompletableFuture
                            .supplyAsync(() -> discover(bundleUnit), pulsar.getExecutor())
                            .thenCompose(broker -> {
                                if (broker.isPresent()) {
                                    log.info("Selected the new owner broker:{} for bundle:{}", broker.get(), bundle);
                                    return bundleStateChannel.publishAssignment(bundle, broker.get());
                                } else {
                                    throw new IllegalStateException(
                                            "Failed to discover(select) the new owner broker for bundle:" + bundle);
                                }
                            });
                }
            }

            owner.thenAccept(broker -> {
                Optional<BrokerLookupData> lookup = getBrokerRegistry().lookup(broker.get());
                if (lookup.isEmpty()) {
                    String errorMsg = String.format(
                            "Failed to look up a broker registry:%s for bundle:%s, activeBrokers:%s",
                            broker, bundle, getBrokerRegistry().getAvailableBrokers());
                    log.error(errorMsg);
                    future.completeExceptionally(new IllegalStateException(errorMsg));
                } else {
                    future.complete(Optional.of(lookup.get().toLookupResult()));
                }
            }).exceptionally(exception -> {
                log.warn("Failed to check owner for bundle {}: {}", bundle, exception.getMessage(), exception);
                future.completeExceptionally(exception);
                return null;
            });

            future.whenComplete((r, t) -> pulsar.getExecutor().execute(
                    () -> lookupRequests.remove(bundle)
            ));

            return future;
        });
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
