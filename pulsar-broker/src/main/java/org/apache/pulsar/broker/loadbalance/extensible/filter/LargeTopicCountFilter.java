package org.apache.pulsar.broker.loadbalance.extensible.filter;

import java.util.Set;

import org.apache.pulsar.broker.loadbalance.extensible.BaseLoadManagerContext;

/**
 * Large topic count filter.
 */
public class LargeTopicCountFilter extends BaseBrokerFilter {

    public static final String FILTER_NAME = "large_topic_count_filter";

    @Override
    void doFilter(Set<String> brokers, BaseLoadManagerContext context) {

    }

    @Override
    public String name() {
        return FILTER_NAME;
    }
}
