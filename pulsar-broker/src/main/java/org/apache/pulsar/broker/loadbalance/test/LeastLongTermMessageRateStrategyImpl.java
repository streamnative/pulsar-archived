package org.apache.pulsar.broker.loadbalance.test;

import java.util.Optional;
import java.util.Set;
import org.apache.pulsar.broker.loadbalance.ResourceUnit;

/**
 * Placement strategy which selects a broker based on which one has the least long term message rate.
 * See {@link org.apache.pulsar.broker.loadbalance.impl.LeastLongTermMessageRate}
 */
public class LeastLongTermMessageRateStrategyImpl extends AbstractBrokerSelectionStrategy {

    public static final String LOAD_BALANCER_NAME = "LeastLongTermMessageRate";

    @Override
    public Optional<ResourceUnit> doSelect(Set<String> brokers, BaseLoadManagerContext context) {
        return Optional.empty();
    }

    @Override
    public String name() {
        return LOAD_BALANCER_NAME;
    }
}
