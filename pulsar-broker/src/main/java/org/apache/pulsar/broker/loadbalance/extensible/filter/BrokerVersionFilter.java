package org.apache.pulsar.broker.loadbalance.extensible.filter;

import org.apache.pulsar.broker.loadbalance.extensible.BaseLoadManagerContext;
import java.util.Set;

/**
 * Filter by broker version.
 */
public class BrokerVersionFilter extends BaseBrokerFilter {

    public static final String FILTER_NAME = "broker_version_filter";

    @Override
    void doFilter(Set<String> brokers, BaseLoadManagerContext context) {

    }

    @Override
    public String name() {
        return FILTER_NAME;
    }
}
