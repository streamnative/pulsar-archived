package org.apache.pulsar.broker.loadbalance.extensible;

import java.util.List;
import java.util.Optional;
import org.apache.commons.collections.CollectionUtils;

public abstract class AbstractBrokerSelectionStrategy implements BrokerSelectionStrategy {

    @Override
    public Optional<String> select(List<String> brokers, LoadManagerContext context) {
        if (CollectionUtils.isEmpty(brokers)) {
            return Optional.empty();
        }

        if (brokers.size() == 1) {
            return Optional.of(brokers.get(0));
        }

        if (!(context instanceof BaseLoadManagerContext)) {
            throw new IllegalStateException("The context must be BaseContext.");
        }

        return doSelect(brokers, (BaseLoadManagerContext) context);
    }

    public abstract Optional<String> doSelect(List<String> brokers, BaseLoadManagerContext context);
}
