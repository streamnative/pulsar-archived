package org.apache.pulsar.broker.loadbalance.extensible.data;

import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import lombok.Getter;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pulsar.policies.data.loadbalancer.NamespaceBundleStats;

@Getter
public class TopBundlesLoadData {

    private final List<Pair<String, NamespaceBundleStats>> topBundlesLoadData;

    private TopBundlesLoadData(Map<String, NamespaceBundleStats> bundleStats, int topK) {
        topBundlesLoadData = bundleStats.entrySet()
                .stream()
                .map(entry -> Pair.of(entry.getKey(), entry.getValue()))
                .sorted(Comparator.naturalOrder())
                .limit(topK)
                .collect(Collectors.toList());
    }

    public static TopBundlesLoadData of(Map<String, NamespaceBundleStats> bundleStats, int topK) {
        return new TopBundlesLoadData(bundleStats, topK);
    }
}
