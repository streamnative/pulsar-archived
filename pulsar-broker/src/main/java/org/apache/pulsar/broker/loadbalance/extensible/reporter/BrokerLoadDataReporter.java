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

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
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
import org.apache.pulsar.policies.data.loadbalancer.SystemResourceUsage;

/**
 * The broker load data reporter.
 */
@Slf4j
public class BrokerLoadDataReporter extends AbstractLoadDataReporter<BrokerLoadData> {

    private final PulsarService pulsar;

    private final ServiceConfiguration conf;

    private final LoadDataStore<BrokerLoadData> brokerLoadDataStore;

    private final BrokerHostUsage brokerHostUsage;

    private final String lookupServiceAddress;

    private final BrokerLoadData localData;

    private final BrokerLoadData lastData;

    public BrokerLoadDataReporter(PulsarService pulsar,
                                  String lookupServiceAddress,
                                  LoadDataStore<BrokerLoadData> brokerLoadDataStore) {
        this.brokerLoadDataStore = brokerLoadDataStore;
        this.lookupServiceAddress = lookupServiceAddress;
        this.pulsar = pulsar;
        this.conf = this.pulsar.getConfiguration();
        if (SystemUtils.IS_OS_LINUX) {
            brokerHostUsage = new LinuxBrokerHostUsageImpl(pulsar);
        } else {
            brokerHostUsage = new GenericBrokerHostUsageImpl(pulsar);
        }
        this.localData = new BrokerLoadData();
        this.lastData = new BrokerLoadData();

    }

    @Override
    public BrokerLoadData generateLoadData() {
        final SystemResourceUsage systemResourceUsage = LoadManagerShared.getSystemResourceUsage(brokerHostUsage);
        localData.update(systemResourceUsage, getBundleStats(pulsar));
        localData.setLastUpdate(System.currentTimeMillis());
        return this.localData;
    }

    @Override
    public CompletableFuture<Void> reportAsync(boolean force) {
        try {
            BrokerLoadData newLoadData = this.generateLoadData();
            if (needBrokerDataUpdate() || force) {
                log.info("publishing load report:{}", localData.printResourceUsage(conf));
                CompletableFuture<Void> future =
                        this.brokerLoadDataStore.pushAsync(this.lookupServiceAddress, newLoadData);
                future.exceptionally(ex -> {
                    log.error("Flush the broker load data failed.", ex);
                    return null;
                });
                return future;
            } else {
                log.info("skipping load report:{}", localData.printResourceUsage(conf));
            }

            return CompletableFuture.completedFuture(null);
        } catch (Throwable e) {
            log.error("Failed to report top broker load data", e);
            return CompletableFuture.failedFuture(e);
        }

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
}
