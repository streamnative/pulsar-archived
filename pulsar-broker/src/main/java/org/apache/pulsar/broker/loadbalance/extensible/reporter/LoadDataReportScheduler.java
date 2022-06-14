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
package org.apache.pulsar.broker.loadbalance.extensible.reporter;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import io.netty.util.concurrent.DefaultThreadFactory;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.SystemUtils;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.loadbalance.BrokerHostUsage;
import org.apache.pulsar.broker.loadbalance.extensible.data.BrokerLoadData;
import org.apache.pulsar.broker.loadbalance.extensible.data.LoadDataStore;
import org.apache.pulsar.broker.loadbalance.impl.GenericBrokerHostUsageImpl;
import org.apache.pulsar.broker.loadbalance.impl.LinuxBrokerHostUsageImpl;
import org.apache.pulsar.broker.loadbalance.impl.LoadManagerShared;
import org.apache.pulsar.common.util.FutureUtil;
import org.apache.pulsar.policies.data.loadbalancer.BundleData;
import org.apache.pulsar.policies.data.loadbalancer.NamespaceBundleStats;
import org.apache.pulsar.policies.data.loadbalancer.SystemResourceUsage;
import org.apache.pulsar.policies.data.loadbalancer.TimeAverageBrokerData;

/**
 * Report the load data to load data store.
 */
@Slf4j
public class LoadDataReportScheduler implements LoadDataReport {

    private final PulsarService pulsar;

    private final ServiceConfiguration conf;

    private final LoadDataStore<BrokerLoadData> brokerLoadDataStore;

    private final LoadDataStore<BundleData> bundleLoadDataStore;

    private final LoadDataStore<TimeAverageBrokerData> timeAverageBrokerLoadDataStore;

    private final ScheduledExecutorService executor;

    private final BrokerHostUsage brokerHostUsage;

    private final String lookupServiceAddress;

    private final BrokerLoadData localData;

    private final BrokerLoadData lastData;

    // BundleName ---> BundleData
    private final Map<String, BundleData> bundleLoadDataMap;

    private volatile ScheduledFuture<?> brokerLoadDataReportFuture;

    private volatile ScheduledFuture<?> bundleLoadDataReportFuture;

    private volatile ScheduledFuture<?> timeAverageBrokerDataReportFuture;

    private final AtomicBoolean started = new AtomicBoolean(false);


    public LoadDataReportScheduler(
            PulsarService pulsar,
            LoadDataStore<BrokerLoadData> brokerLoadDataStore,
            LoadDataStore<BundleData> bundleLoadDataStore,
            LoadDataStore<TimeAverageBrokerData> timeAverageBrokerLoadDataStore,
            String lookupServiceAddress) {
        this.brokerLoadDataStore = brokerLoadDataStore;
        this.bundleLoadDataStore = bundleLoadDataStore;
        this.timeAverageBrokerLoadDataStore = timeAverageBrokerLoadDataStore;
        this.lookupServiceAddress = lookupServiceAddress;
        this.pulsar = pulsar;
        this.conf = this.pulsar.getConfiguration();
        if (SystemUtils.IS_OS_LINUX) {
            brokerHostUsage = new LinuxBrokerHostUsageImpl(pulsar);
        } else {
            brokerHostUsage = new GenericBrokerHostUsageImpl(pulsar);
        }
        this.executor = Executors
                .newSingleThreadScheduledExecutor(new DefaultThreadFactory("load-data-report-executor"));
        this.localData = new BrokerLoadData();
        this.lastData = new BrokerLoadData();
        this.bundleLoadDataMap = new ConcurrentHashMap<>();
    }

    @Override
    public void start() {
        if (started.compareAndSet(false, true)) {
            long loadReportMinInterval = LoadManagerShared.LOAD_REPORT_UPDATE_MINIMUM_INTERVAL;
            this.brokerLoadDataReportFuture = executor.scheduleAtFixedRate(this::reportBrokerLoadDataAsync,
                    loadReportMinInterval,
                    loadReportMinInterval,
                    TimeUnit.MILLISECONDS);

            long resourceQuotaUpdateInterval = TimeUnit.MINUTES
                    .toMillis(this.conf.getLoadBalancerResourceQuotaUpdateIntervalMinutes());
            this.bundleLoadDataReportFuture = executor.scheduleAtFixedRate(this::reportBundleLoadDataAsync,
                    resourceQuotaUpdateInterval,
                    resourceQuotaUpdateInterval,
                    TimeUnit.MINUTES);

            this.timeAverageBrokerDataReportFuture =
                    executor.scheduleAtFixedRate(this::reportTimeAverageBrokerDataAsync,
                    resourceQuotaUpdateInterval,
                    resourceQuotaUpdateInterval,
                    TimeUnit.MINUTES);
        }

    }

    @Override
    public void close() {
        if (started.compareAndSet(true, false)) {
            if (this.brokerLoadDataReportFuture != null) {
                this.brokerLoadDataReportFuture.cancel(false);
            }
            if (this.bundleLoadDataReportFuture != null) {
                this.bundleLoadDataReportFuture.cancel(false);
            }
            if (this.timeAverageBrokerDataReportFuture != null) {
                this.timeAverageBrokerDataReportFuture.cancel(false);
            }
        }
    }

    @Override
    public CompletableFuture<Void> reportBrokerLoadDataAsync() {
        if (needBrokerDataUpdate()) {
            CompletableFuture<Void> future =
                    this.brokerLoadDataStore.pushAsync(this.lookupServiceAddress, this.generateBrokerLoadData());
            future.exceptionally(ex -> {
                log.error("Flush the broker load data failed.", ex);
                return null;
            });
            return future;
        }
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public CompletableFuture<Void> reportBundleLoadDataAsync() {
        List<CompletableFuture<Void>> futureList = new ArrayList<>();
        this.generateBundleData().forEach((bundle, bundleData) -> {
            futureList.add(this.bundleLoadDataStore.pushAsync(bundle, bundleData));
        });
        return FutureUtil.waitForAll(futureList).thenAccept(__ -> {}).exceptionally(ex -> {
            log.error("Report the bundle load data failed.", ex);
            return null;
        }).thenAccept(__ -> {});
    }

    @Override
    public CompletableFuture<Void> reportTimeAverageBrokerDataAsync() {
        CompletableFuture<Void> future = this.timeAverageBrokerLoadDataStore.pushAsync(
                this.lookupServiceAddress,
                this.generateTimeAverageBrokerData());
        future.exceptionally(ex -> {
            log.error("Flush the time average broker data failed.", ex);
            return null;
        });
        return future;
    }

    @Override
    public BrokerLoadData generateBrokerLoadData() {
        final SystemResourceUsage systemResourceUsage = LoadManagerShared.getSystemResourceUsage(brokerHostUsage);
        localData.update(systemResourceUsage, getBundleStats());
        localData.setLastUpdate(System.currentTimeMillis());
        return this.localData;
    }

    @Override
    public synchronized Map<String, BundleData> generateBundleData() {
        this.bundleLoadDataMap.clear();
        Map<String, NamespaceBundleStats> statsMap = this.getBundleStats();
        statsMap.forEach((bundle, stats) -> {
            BundleData currentBundleData =
                    this.bundleLoadDataMap.getOrDefault(bundle,
                            bundleLoadDataStore.get(bundle).orElse(BundleData.newDefaultBundleData()));
            currentBundleData.update(stats);
            this.bundleLoadDataMap.put(bundle, currentBundleData);
        });
        return this.bundleLoadDataMap;
    }

    @Override
    public TimeAverageBrokerData generateTimeAverageBrokerData() {
        TimeAverageBrokerData timeAverageBrokerData =
                timeAverageBrokerLoadDataStore.get(lookupServiceAddress).orElse(new TimeAverageBrokerData());
        timeAverageBrokerData.reset(this.getBundleStats().keySet(), this.bundleLoadDataMap,
                NamespaceBundleStats.newDefaultNamespaceBundleStats());
        return timeAverageBrokerData;
    }

    private Map<String, NamespaceBundleStats> getBundleStats() {
        return pulsar.getBrokerService().getBundleStats();
    }

    private boolean needBrokerDataUpdate() {
        int loadBalancerReportUpdateMaxIntervalMinutes = conf.getLoadBalancerReportUpdateMaxIntervalMinutes();
        int loadBalancerReportUpdateThresholdPercentage = conf.getLoadBalancerReportUpdateThresholdPercentage();
        final long updateMaxIntervalMillis = TimeUnit.MINUTES
                .toMillis(loadBalancerReportUpdateMaxIntervalMinutes);
        long timeSinceLastReportWrittenToStore = System.currentTimeMillis() - localData.getLastUpdate();
        if (timeSinceLastReportWrittenToStore > updateMaxIntervalMillis) {
            log.info("Writing local data to metadata store because time since last"
                            + " update exceeded threshold of {} minutes",
                    loadBalancerReportUpdateMaxIntervalMinutes);
            // Always update after surpassing the maximum interval.
            return true;
        }
        final double maxChange = Math
                .max(100.0 * (Math.abs(lastData.getMaxResourceUsage() - localData.getMaxResourceUsage())),
                        Math.max(percentChange(lastData.getMsgRateIn() + lastData.getMsgRateOut(),
                                        localData.getMsgRateIn() + localData.getMsgRateOut()),
                                Math.max(
                                        percentChange(lastData.getMsgThroughputIn() + lastData.getMsgThroughputOut(),
                                                localData.getMsgThroughputIn() + localData.getMsgThroughputOut()),
                                        percentChange(lastData.getNumBundles(), localData.getNumBundles()))));
        if (maxChange > loadBalancerReportUpdateThresholdPercentage) {
            log.info("Writing local data to metadata store because maximum change {}% exceeded threshold {}%; "
                            + "time since last report written is {} seconds", maxChange,
                    loadBalancerReportUpdateThresholdPercentage,
                    timeSinceLastReportWrittenToStore / 1000.0);
            return true;
        }
        return false;
    }

    protected double percentChange(final double oldValue, final double newValue) {
        if (oldValue == 0) {
            if (newValue == 0) {
                // Avoid NaN
                return 0;
            }
            return Double.POSITIVE_INFINITY;
        }
        return 100 * Math.abs((oldValue - newValue) / oldValue);
    }
}
