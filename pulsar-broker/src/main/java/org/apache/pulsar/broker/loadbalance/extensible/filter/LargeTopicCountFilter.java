package org.apache.pulsar.broker.loadbalance.extensible.filter;

import java.util.List;
import java.util.stream.Collectors;
import org.apache.pulsar.broker.loadbalance.extensible.BaseLoadManagerContext;
import org.apache.pulsar.broker.loadbalance.extensible.data.BrokerLoadData;
import org.apache.pulsar.policies.data.loadbalancer.BundleData;

/**
 * Large topic count filter.
 */
public class LargeTopicCountFilter extends BaseBrokerFilter {

    public static final String FILTER_NAME = "large_topic_count_filter";

    @Override
    void doFilter(List<String> brokers, BaseLoadManagerContext context) {
        int loadBalancerBrokerMaxTopics = context.brokerConfiguration().getLoadBalancerBrokerMaxTopics();
        List<String> filteredBrokerCandidates = brokers.stream().filter((broker) -> {
            BrokerLoadData brokerLoadData = context.brokerLoadDataStore().get(broker);
            long totalTopics = context.preallocatedBundleData(broker)
                    .values()
                    .stream()
                    .mapToLong(BundleData::getTopics).sum()
                    + (brokerLoadData == null ? 0 : brokerLoadData.getNumTopics());
            return totalTopics <= loadBalancerBrokerMaxTopics;
        }).collect(Collectors.toList());

        if (!filteredBrokerCandidates.isEmpty()) {
            brokers.clear();
            brokers.addAll(filteredBrokerCandidates);
        }
    }

    @Override
    public String name() {
        return FILTER_NAME;
    }
}
