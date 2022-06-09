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

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import lombok.Getter;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.loadbalance.BrokerFilterException;
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
import org.apache.pulsar.broker.loadbalance.extensible.strategy.BrokerSelectionStrategy;
import org.apache.pulsar.broker.loadbalance.extensible.strategy.LeastLongTermMessageRateStrategyImpl;
import org.apache.pulsar.common.naming.ServiceUnitId;
import org.apache.pulsar.policies.data.loadbalancer.BundleData;
import org.apache.pulsar.policies.data.loadbalancer.TimeAverageBrokerData;

/**
 * The broker discovery implementation.
 */
public class BrokerDiscoveryImpl implements BrokerDiscovery {

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

    public BrokerDiscoveryImpl() {}

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

        // When the system namespace doing the bundle lookup,
        // the load manager might not start yet, so we return a random broker.
        if (!started.get()) {
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

        BrokerSelectionStrategy brokerSelectionStrategy = getBrokerSelectionStrategy(serviceUnit);

        Optional<String> selectedBroker = brokerSelectionStrategy.select(availableBrokers, context);
        Optional<BundleData> bundleDataOpt = bundleLoadDataStore.get(bundle);
        context.preallocatedBundleData(selectedBroker.get())
                .put(bundle, bundleDataOpt.orElse(BundleData.newDefaultBundleData()));
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
