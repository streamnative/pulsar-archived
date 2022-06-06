package org.apache.pulsar.broker.loadbalance.extensible;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import org.apache.commons.collections.CollectionUtils;
import org.apache.pulsar.broker.loadbalance.ResourceUnit;
import org.apache.pulsar.broker.loadbalance.impl.PulsarResourceDescription;
import org.apache.pulsar.broker.loadbalance.impl.SimpleResourceUnit;

public abstract class AbstractBrokerSelectionStrategy implements BrokerSelectionStrategy {

    @Override
    public Optional<ResourceUnit> select(Set<String> brokers, LoadManagerContext context) {
        if (CollectionUtils.isEmpty(brokers)) {
            return Optional.empty();
        }

        if (brokers.size() == 1) {
            List<String> brokersList = new ArrayList<>(brokers);
            return Optional.of(new SimpleResourceUnit(brokersList.get(0), new PulsarResourceDescription()));
        }

        if (!(context instanceof BaseLoadManagerContext)) {
            throw new IllegalStateException("The context must be BaseContext.");
        }

        return doSelect(brokers, (BaseLoadManagerContext) context);
    }

    public abstract Optional<ResourceUnit> doSelect(Set<String> brokers, BaseLoadManagerContext context);
}
