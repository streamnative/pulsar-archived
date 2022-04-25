package org.apache.pulsar.broker.loadbalance.test.filter;

import java.util.Set;
import org.apache.pulsar.broker.loadbalance.test.BaseLoadManagerContext;
import org.apache.pulsar.broker.loadbalance.test.LoadManagerContext;

/**
 * The base broker filter only use BaseContext.
 */
public abstract class BaseBrokerFilter implements BrokerFilter {

    abstract void doFilter(Set<String> brokers, BaseLoadManagerContext context);

    @Override
    public void filter(Set<String> brokers, LoadManagerContext context) {
        if (context instanceof BaseLoadManagerContext) {
            this.doFilter(brokers, (BaseLoadManagerContext) context);
        } else {
            throw new IllegalArgumentException("The context must be BaseContext.");
        }
    }
}
