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
package org.apache.pulsar.broker.loadbalance.extensible.data;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import lombok.Data;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.naming.TopicDomain;
import org.apache.pulsar.policies.data.loadbalancer.LocalBrokerData;
import org.apache.pulsar.policies.data.loadbalancer.NamespaceBundleStats;
import org.apache.pulsar.policies.data.loadbalancer.ResourceUsage;
import org.apache.pulsar.policies.data.loadbalancer.SystemResourceUsage;

/**
 * Contains all the data that is maintained locally on each broker.
 */
@Data
public class BrokerLoadData {

    public static final String TOPIC =
            TopicDomain.non_persistent
                    + "://"
                    + NamespaceName.SYSTEM_NAMESPACE
                    + "/broker-load-data";

    private static final double gigaBitToByte = 128 * 1024 * 1024.0;

    // Most recently available system resource usage.
    private ResourceUsage cpu;
    private ResourceUsage memory;
    private ResourceUsage directMemory;

    private ResourceUsage bandwidthIn;
    private ResourceUsage bandwidthOut;

    // Message data from the most recent namespace bundle stats.
    private double msgThroughputIn;
    private ResourceUsage msgThroughputInUsage;
    private double msgThroughputOut;
    private ResourceUsage msgThroughputOutUsage;
    private double msgRateIn;
    private double msgRateOut;

    // Timestamp of last update.
    private long lastUpdate;

    // The stats given in the most recent invocation of update.
    private Map<String, NamespaceBundleStats> lastStats;

    private int numTopics;
    private int numBundles;
    private int numConsumers;
    private int numProducers;

    // All bundles belonging to this broker.
    private Set<String> bundles;

    // The bundles gained since the last invocation of update.
    private Set<String> lastBundleGains;

    // The bundles lost since the last invocation of update.
    private Set<String> lastBundleLosses;

    public BrokerLoadData() {
        lastStats = new ConcurrentHashMap<>();
        lastUpdate = System.currentTimeMillis();
        cpu = new ResourceUsage();
        memory = new ResourceUsage();
        directMemory = new ResourceUsage();
        bandwidthIn = new ResourceUsage();
        bandwidthOut = new ResourceUsage();
        msgThroughputInUsage = new ResourceUsage();
        msgThroughputOutUsage = new ResourceUsage();
        bundles = new HashSet<>();
        lastBundleGains = new HashSet<>();
        lastBundleLosses = new HashSet<>();
    }

    /**
     * Using the system resource usage and bundle stats acquired from the Pulsar client, update this LocalBrokerData.
     *
     * @param systemResourceUsage
     *            System resource usage (cpu, memory, and direct memory).
     * @param bundleStats
     *            The bundle stats retrieved from the Pulsar client.
     */
    public void update(final SystemResourceUsage systemResourceUsage,
                       final Map<String, NamespaceBundleStats> bundleStats) {
        updateSystemResourceUsage(systemResourceUsage);
        updateBundleData(bundleStats);
        lastStats = bundleStats;
    }

    /**
     * Using another LocalBrokerData, update this.
     *
     * @param other
     *            LocalBrokerData to update from.
     */
    public void update(final BrokerLoadData other) {
        updateSystemResourceUsage(other.cpu, other.memory, other.directMemory, other.bandwidthIn, other.bandwidthOut);
        updateBundleData(other.lastStats);
        lastStats = other.lastStats;
    }

    // Set the cpu, memory, and direct memory to that of the new system resource usage data.
    private void updateSystemResourceUsage(final SystemResourceUsage systemResourceUsage) {
        updateSystemResourceUsage(systemResourceUsage.cpu, systemResourceUsage.memory, systemResourceUsage.directMemory,
                systemResourceUsage.bandwidthIn, systemResourceUsage.bandwidthOut);
    }

    // Update resource usage given each individual usage.
    private void updateSystemResourceUsage(final ResourceUsage cpu, final ResourceUsage memory,
                                           final ResourceUsage directMemory, final ResourceUsage bandwidthIn,
                                           final ResourceUsage bandwidthOut) {
        this.cpu = cpu;
        this.memory = memory;
        this.directMemory = directMemory;
        this.bandwidthIn = bandwidthIn;
        this.bandwidthOut = bandwidthOut;
    }

    // Aggregate all message, throughput, topic count, bundle count, consumer
    // count, and producer count across the
    // given data. Also keep track of bundle gains and losses.
    private void updateBundleData(final Map<String, NamespaceBundleStats> bundleStats) {
        msgRateIn = 0;
        msgRateOut = 0;
        msgThroughputIn = 0;
        msgThroughputOut = 0;
        int totalNumTopics = 0;
        int totalNumBundles = 0;
        int totalNumConsumers = 0;
        int totalNumProducers = 0;
        final Iterator<String> oldBundleIterator = bundles.iterator();
        while (oldBundleIterator.hasNext()) {
            final String bundle = oldBundleIterator.next();
            if (!bundleStats.containsKey(bundle)) {
                // If this bundle is in the old bundle set but not the new one,
                // we lost it.
                lastBundleLosses.add(bundle);
                oldBundleIterator.remove();
            }
        }
        for (Map.Entry<String, NamespaceBundleStats> entry : bundleStats.entrySet()) {
            final String bundle = entry.getKey();
            final NamespaceBundleStats stats = entry.getValue();
            if (!bundles.contains(bundle)) {
                // If this bundle is in the new bundle set but not the old one,
                // we gained it.
                lastBundleGains.add(bundle);
                bundles.add(bundle);
            }
            msgThroughputIn += stats.msgThroughputIn;
            msgThroughputOut += stats.msgThroughputOut;
            msgRateIn += stats.msgRateIn;
            msgRateOut += stats.msgRateOut;
            totalNumTopics += stats.topics;
            ++totalNumBundles;
            totalNumConsumers += stats.consumerCount;
            totalNumProducers += stats.producerCount;
        }
        numTopics = totalNumTopics;
        numBundles = totalNumBundles;
        numConsumers = totalNumConsumers;
        numProducers = totalNumProducers;
    }

    public void cleanDeltas() {
        lastBundleGains.clear();
        lastBundleLosses.clear();
    }

    public double getMaxResourceUsage() {
        return max(
                cpu.percentUsage(),
                memory.percentUsage(),
                directMemory.percentUsage(),
                bandwidthIn.percentUsage(),
                bandwidthOut.percentUsage())
                / 100;
    }

    public double getMaxResourceUsageWithinLimit(ServiceConfiguration conf) {
        return maxWithinLimit(100.0d,
                cpu.percentUsage() * conf.getLoadBalancerCPUResourceWeight(),
                memory.percentUsage() * conf.getLoadBalancerMemoryResourceWeight(),
                directMemory.percentUsage() * conf.getLoadBalancerDirectMemoryResourceWeight(),
                bandwidthIn.percentUsage() * conf.getLoadBalancerBandwithInResourceWeight(),
                bandwidthOut.percentUsage() * conf.getLoadBalancerBandwithOutResourceWeight())
                / 100;
    }

    private static double getNicSpeedBytesInSec(ServiceConfiguration conf) {
        return conf.getLoadBalancerOverrideBrokerNicSpeedGbps().isPresent()
                ? conf.getLoadBalancerOverrideBrokerNicSpeedGbps().get() * gigaBitToByte : -1.0;
    }

    synchronized ResourceUsage getMsgThroughputInUsage(double nicSpeedBytesInSec) {
        if (msgThroughputInUsage.usage != msgThroughputIn) {
            msgThroughputInUsage = new ResourceUsage(msgThroughputIn, nicSpeedBytesInSec);
        }
        return msgThroughputInUsage;
    }

    synchronized ResourceUsage getMsgThroughputOutUsage(double nicSpeedBytesInSec) {
        if (msgThroughputOutUsage.usage != msgThroughputOut) {
            msgThroughputOutUsage = new ResourceUsage(msgThroughputOut, nicSpeedBytesInSec);
        }
        return msgThroughputOutUsage;
    }

    public double getMaxResourceUsageWithExtendedNetworkSignal(ServiceConfiguration conf) {

        double nicSpeedBytesInSec = getNicSpeedBytesInSec(conf);
        return maxWithinLimit(100.0d,
                cpu.percentUsage() * conf.getLoadBalancerCPUResourceWeight(),
                memory.percentUsage() * conf.getLoadBalancerMemoryResourceWeight(),
                directMemory.percentUsage() * conf.getLoadBalancerDirectMemoryResourceWeight(),
                bandwidthIn.percentUsage() * conf.getLoadBalancerBandwithInResourceWeight(),
                bandwidthOut.percentUsage() * conf.getLoadBalancerBandwithOutResourceWeight(),
                getMsgThroughputInUsage(nicSpeedBytesInSec).percentUsage()
                        * conf.getLoadBalancerBandwithInResourceWeight(),
                getMsgThroughputOutUsage(nicSpeedBytesInSec).percentUsage()
                        * conf.getLoadBalancerBandwithOutResourceWeight())
                / 100;
    }

    public double getMaxResourceUsage(ServiceConfiguration conf) {
        return max(
                cpu.percentUsage() * conf.getLoadBalancerCPUResourceWeight(),
                memory.percentUsage() * conf.getLoadBalancerMemoryResourceWeight(),
                directMemory.percentUsage() * conf.getLoadBalancerDirectMemoryResourceWeight(),
                bandwidthIn.percentUsage() * conf.getLoadBalancerBandwithInResourceWeight(),
                bandwidthOut.percentUsage() * conf.getLoadBalancerBandwithOutResourceWeight())
                / 100;
    }

    private static double maxWithinLimit(double limit, double...args) {
        double max = 0.0;
        for (double d : args) {
            if (d > max && d <= limit) {
                max = d;
            }
        }
        return max;
    }

    public String printResourceUsage(ServiceConfiguration conf) {
        double nicSpeedBytesInSec = getNicSpeedBytesInSec(conf);
        return String.format(
                Locale.ENGLISH,
                "cpu: %.2f%%, memory: %.2f%%, directMemory: %.2f%%,"
                        + " bandwidthIn: %.2f%%, bandwidthOut: %.2f%%,"
                        + " MsgThroughputIn: %.2f%%, MsgThroughputOut: %.2f%%",
                cpu.percentUsage(), memory.percentUsage(), directMemory.percentUsage(),
                bandwidthIn.percentUsage(),
                bandwidthOut.percentUsage(),
                getMsgThroughputInUsage(nicSpeedBytesInSec).percentUsage(),
                getMsgThroughputOutUsage(nicSpeedBytesInSec).percentUsage());
    }

    private static double max(double...args) {
        double max = Double.NEGATIVE_INFINITY;

        for (double d : args) {
            if (d > max) {
                max = d;
            }
        }

        return max;
    }

    public static LocalBrokerData convertToLoadManagerReport(BrokerLoadData brokerLoadData) {
        LocalBrokerData localBrokerData = new LocalBrokerData();
        localBrokerData.setCpu(brokerLoadData.getCpu());
        localBrokerData.setMemory(brokerLoadData.getMemory());
        localBrokerData.setBandwidthIn(brokerLoadData.getBandwidthIn());
        localBrokerData.setBandwidthOut(brokerLoadData.getBandwidthOut());
        localBrokerData.setBundles(brokerLoadData.getBundles());
        localBrokerData.setDirectMemory(brokerLoadData.getDirectMemory());
        localBrokerData.setLastStats(brokerLoadData.getLastStats());
        localBrokerData.setLastBundleGains(brokerLoadData.getLastBundleGains());
        localBrokerData.setLastBundleLosses(brokerLoadData.getLastBundleLosses());
        localBrokerData.setLastUpdate(brokerLoadData.getLastUpdate());
        localBrokerData.setNumBundles(brokerLoadData.getNumBundles());
        localBrokerData.setNumTopics(brokerLoadData.getNumTopics());
        localBrokerData.setNumProducers(brokerLoadData.getNumProducers());
        localBrokerData.setNumConsumers(brokerLoadData.getNumConsumers());
        localBrokerData.setMsgRateIn(brokerLoadData.getMsgRateIn());
        localBrokerData.setMsgRateOut(brokerLoadData.getMsgRateOut());
        localBrokerData.setMsgThroughputIn(brokerLoadData.getMsgThroughputIn());
        localBrokerData.setMsgThroughputOut(brokerLoadData.getMsgThroughputOut());
        return localBrokerData;
    }
}
