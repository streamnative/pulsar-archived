package org.apache.pulsar.broker.loadbalance.test.data;

import java.util.Map;
import java.util.Set;
import lombok.Data;
import org.apache.pulsar.policies.data.loadbalancer.NamespaceBundleStats;
import org.apache.pulsar.policies.data.loadbalancer.ResourceUsage;

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
}
