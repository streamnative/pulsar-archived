package org.apache.pulsar.broker.loadbalance.extensible.strategy;

import org.apache.pulsar.broker.loadbalance.extensible.BaseLoadManagerContext;

import java.util.List;
import java.util.Optional;

public class LeastResourceUsageWithWeightBrokerSelectionStrategy extends AbstractBrokerSelectionStrategy {

    @Override
    public Optional<String> doSelect(List<String> brokers, BaseLoadManagerContext context) {
        return Optional.empty();
    }

    @Override
    public String name() {
        return "LeastResourceUsageWithWeight";
    }
}
