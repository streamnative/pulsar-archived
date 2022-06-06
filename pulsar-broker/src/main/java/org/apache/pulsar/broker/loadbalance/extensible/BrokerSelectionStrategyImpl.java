package org.apache.pulsar.broker.loadbalance.extensible;

import org.apache.pulsar.broker.loadbalance.ResourceUnit;

import java.util.Optional;
import java.util.Set;

public class BrokerSelectionStrategyImpl extends AbstractBrokerSelectionStrategy {

    @Override
    public Optional<ResourceUnit> doSelect(Set<String> brokers, BaseLoadManagerContext context) {

        return Optional.empty();
    }

    @Override
    public String name() {
        return "default";
    }
}
