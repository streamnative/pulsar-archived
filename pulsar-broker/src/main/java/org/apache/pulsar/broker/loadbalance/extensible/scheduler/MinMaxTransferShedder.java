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
package org.apache.pulsar.broker.loadbalance.extensible.scheduler;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import lombok.AllArgsConstructor;
import lombok.ToString;
import org.apache.commons.lang3.mutable.MutableBoolean;
import org.apache.commons.lang3.mutable.MutableDouble;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.loadbalance.extensible.BaseLoadManagerContext;
import org.apache.pulsar.broker.loadbalance.extensible.data.BrokerLoadData;
import org.apache.pulsar.broker.loadbalance.extensible.data.LoadDataStore;
import org.apache.pulsar.broker.loadbalance.extensible.data.TopBundlesLoadData;
import org.apache.pulsar.broker.loadbalance.extensible.data.Unload;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

//TODO: add doc
public class MinMaxTransferShedder implements NamespaceUnloadStrategy {
    private static final Logger log = LoggerFactory.getLogger(MinMaxTransferShedder.class);
    private final List<Unload> selectedBundlesCache = new ArrayList<>();
    private static final double KB = 1024;

    private static final long LOAD_LOG_SAMPLE_DELAY_IN_SEC = 5 * 60; // 5 mins
    private final Map<String, Double> brokerAvgResourceUsage = new HashMap<>();
    private long lastSampledLoadLogTS = 0;


    private static int toPercentage(double usage) {
        return (int) (usage * 100);
    }

    private boolean canSampleLog() {
        long now = System.currentTimeMillis() / 1000;
        boolean sampleLog = now - lastSampledLoadLogTS >= LOAD_LOG_SAMPLE_DELAY_IN_SEC;
        if (sampleLog) {
            lastSampledLoadLogTS = now;
        }
        return sampleLog;
    }

    @AllArgsConstructor
    @ToString
    private static class LoadStats {
        double avg;
        double std;

        String minBroker;
        double min;
        String maxBroker;
        double max;


        @Override
        public String toString() {
            return String.format("avg:%.2f, std:%.2f, minBroker:%s, min:%.2f, maxBroker:%s, max:%.2f",
                    avg, std, minBroker, min, maxBroker, max);
        }
    }

    @Override
    public List<Unload> findBundlesForUnloading(BaseLoadManagerContext context,
                                                Map<String, Long> recentlyUnloadedBundles) {
        final var conf = context.brokerConfiguration();
        selectedBundlesCache.clear();
        boolean sampleLog = canSampleLog();

        final LoadStats stats = computeStats(
                context.brokerLoadDataStore(), conf.getLoadBalancerHistoryResourcePercentage(), conf, sampleLog);
        if (sampleLog) {
            log.info("brokers' load stats:{}", stats);
        }

        if (stats.maxBroker == null) {
            log.warn("no target broker is found. skip unloading. stats:{}", stats);
            return selectedBundlesCache;
        }

        final double threshold = conf.getLoadBalancerBrokerMinMaxTransferShedderThreshold();
        if (stats.std <= threshold) {
            if (sampleLog) {
                log.info("std:{} <= threshold:{}. skip unloading.", stats.std, threshold);
            }
            return selectedBundlesCache;
        }

        boolean transfer = conf.isLoadBalancerSheddingTransferEnabled();
        if (transfer
                && stats.maxBroker != null
                && stats.maxBroker.equals(stats.minBroker)) {
            if (sampleLog) {
                log.info("maxBroker:{} = minBroker:{}. skip unloading.", stats.maxBroker, stats.maxBroker);
            }
            return selectedBundlesCache;
        }

        Optional<BrokerLoadData> maxBrokerLoadData = context.brokerLoadDataStore().get(stats.maxBroker);
        if (maxBrokerLoadData.isEmpty()) {
            log.error("maxBrokerLoadData is empty. skip unloading. stats:{}", stats);
            return selectedBundlesCache;
        }
        double offload = (stats.max - stats.min) / 2;
        BrokerLoadData brokerLoadData = maxBrokerLoadData.get();
        double brokerThroughput = brokerLoadData.getMsgThroughputIn() + brokerLoadData.getMsgThroughputOut();
        double offloadThroughput = brokerThroughput * offload;

        log.info(
                "Attempting to shed load from broker {}, which has the max resource "
                        + "usage {}%, stats:{}, std_threshold:{},"
                        + " -- Offloading {}%, at least {} KByte/s of traffic, left throughput {} KByte/s",
                stats.maxBroker, 100 * stats.max, stats, threshold,
                offload * 100, offloadThroughput / KB, (brokerThroughput - offloadThroughput) / KB);

        MutableDouble trafficMarkedToOffload = new MutableDouble(0);
        MutableBoolean atLeastOneBundleSelected = new MutableBoolean(false);

        Optional<TopBundlesLoadData> bundlesLoadData = context.topBundleLoadDataStore().get(stats.maxBroker);
        if (bundlesLoadData.isEmpty() || bundlesLoadData.get().getTopBundlesLoadData().isEmpty()) {
            log.error("topBundlesLoadData is empty. skip unloading.");
            return selectedBundlesCache;
        }

        var topBundlesLoadData = bundlesLoadData.get().getTopBundlesLoadData();
        if (brokerLoadData.getNumBundles() > 1) {
            topBundlesLoadData.stream()
                    .map((e) -> {
                        String bundle = e.getBundleName();
                        var bundleData = e.getStats();
                        double throughput = bundleData.msgThroughputIn + bundleData.msgThroughputOut;
                        return Pair.of(bundle, throughput);
                    }).filter(e ->
                            !recentlyUnloadedBundles.containsKey(e.getLeft())
                    ).sorted((e1, e2) ->
                            Double.compare(e2.getRight(), e1.getRight())
                    ).forEach(e -> {
                        if (trafficMarkedToOffload.doubleValue() < offloadThroughput
                                || atLeastOneBundleSelected.isFalse()) {
                            if (transfer) {
                                selectedBundlesCache.add(
                                        new Unload(stats.maxBroker, e.getLeft(),
                                                Optional.of(stats.minBroker)));
                            } else {
                                selectedBundlesCache.add(
                                        new Unload(stats.maxBroker, e.getLeft()));
                            }

                            trafficMarkedToOffload.add(e.getRight());
                            atLeastOneBundleSelected.setTrue();
                        }
                    });
        } else if (brokerLoadData.getNumBundles() == 1) {
            log.warn(
                    "HIGH USAGE WARNING : Sole namespace bundle {} is overloading broker {}. "
                            + "No Load Shedding will be done on this broker",
                    topBundlesLoadData.iterator().next(), stats.maxBroker);
        } else {
            log.warn("Broker {} is overloaded despite having no bundles", stats.maxBroker);
        }

        return selectedBundlesCache;
    }

    private LoadStats computeStats(final LoadDataStore<BrokerLoadData> loadData, final double historyPercentage,
                                   final ServiceConfiguration conf, boolean sampleLog) {
        double sum = 0.0;
        double sqSum = 0.0;
        int totalBrokers = 0;
        double min = 100.0;
        String minBroker = null;
        double max = 0.0;
        String maxBroker = null;

        for (Map.Entry<String, BrokerLoadData> entry : loadData.entrySet()) {
            BrokerLoadData localBrokerData = entry.getValue();
            String broker = entry.getKey();
            double load = updateAvgResourceUsage(broker, localBrokerData, historyPercentage, conf, sampleLog);
            if (load < min) {
                minBroker = broker;
                min = load;
            }
            if (load > max) {
                maxBroker = broker;
                max = load;
            }
            sum += load;
            sqSum += load * load;
            totalBrokers++;
        }


        if (totalBrokers == 0) {
            return new LoadStats(0.0, 0.0, minBroker, min, maxBroker, max);
        }

        double avg = sum / totalBrokers;
        double std = Math.sqrt(sqSum / totalBrokers - avg * avg);
        return new LoadStats(avg, std, minBroker, min, maxBroker, max);
    }

    private double updateAvgResourceUsage(String broker, BrokerLoadData brokerLoadData,
                                          final double historyPercentage, final ServiceConfiguration conf,
                                          boolean sampleLog) {
        Double historyUsage =
                brokerAvgResourceUsage.get(broker);
        double resourceUsage = brokerLoadData.getMaxResourceUsageWithExtendedNetworkSignal(conf);

        if (sampleLog) {
            log.info("{} broker load: historyUsage={}%, resourceUsage={}%",
                    broker,
                    historyUsage == null ? 0 : toPercentage(historyUsage),
                    toPercentage(resourceUsage));
        }

        // wrap if resourceUsage is bigger than 1.0
        if (resourceUsage > 1.0) {
            log.error("{} broker resourceUsage is bigger than 100%. "
                            + "Some of the resource limits are mis-configured. "
                            + "Try to disable the error resource signals by setting their weights to zero "
                            + "or fix the resource limit configurations. "
                            + "Ref:https://pulsar.apache.org/docs/administration-load-balance/#thresholdshedder "
                            + "ResourceUsage:[{}], "
                            + "CPUResourceWeight:{}, MemoryResourceWeight:{}, DirectMemoryResourceWeight:{}, "
                            + "BandwithInResourceWeight:{}, BandwithOutResourceWeight:{}",
                    broker,
                    brokerLoadData.printResourceUsage(conf),
                    conf.getLoadBalancerCPUResourceWeight(),
                    conf.getLoadBalancerMemoryResourceWeight(),
                    conf.getLoadBalancerDirectMemoryResourceWeight(),
                    conf.getLoadBalancerBandwithInResourceWeight(),
                    conf.getLoadBalancerBandwithOutResourceWeight());

            resourceUsage = brokerLoadData.getMaxResourceUsageWithinLimit(conf);

            log.warn("{} broker recomputed max resourceUsage={}%. Skipped usage signals bigger than 100%",
                    broker, toPercentage(resourceUsage));
        }
        historyUsage = historyUsage == null
                ? resourceUsage : historyUsage * historyPercentage + (1 - historyPercentage) * resourceUsage;

        brokerAvgResourceUsage.put(broker, historyUsage);
        return historyUsage;
    }
}
