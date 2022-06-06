package org.apache.pulsar.broker.loadbalance.extensible.data;

import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import lombok.Data;
import org.apache.pulsar.policies.data.loadbalancer.NamespaceBundleStats;
import org.apache.pulsar.policies.data.loadbalancer.ResourceUsage;
import org.apache.pulsar.policies.data.loadbalancer.SystemResourceUsage;

@Data
public class BrokerLoadData {
    // Most recently available system resource usage.
    private ResourceUsage cpu;
    private ResourceUsage memory;
    private ResourceUsage directMemory;

    private ResourceUsage bandwidthIn;
    private ResourceUsage bandwidthOut;

    // Message data from the most recent namespace bundle stats.
    private double msgThroughputIn;
    private double msgThroughputOut;
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
                                           final ResourceUsage directMemory, final ResourceUsage bandwidthIn, final ResourceUsage bandwidthOut) {
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
        return max(cpu.percentUsage(), memory.percentUsage(), directMemory.percentUsage(), bandwidthIn.percentUsage(),
                bandwidthOut.percentUsage()) / 100;
    }

    private static float max(float...args) {
        float max = Float.NEGATIVE_INFINITY;

        for (float d : args) {
            if (d > max) {
                max = d;
            }
        }

        return max;
    }
}
