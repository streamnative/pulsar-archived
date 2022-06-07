package org.apache.pulsar.broker.loadbalance.extensible;

import java.util.List;
import java.util.Optional;

/**
 * The broker selection strategy is designed to select the broker according to different implementations.
 */
public interface BrokerSelectionStrategy {

    /**
     * The load balancer name.
     *
     * @return load balancer name.
     */
    String name();

    /**
     * Choose an appropriate broker according to different load balancing implementations.
     *
     * @param brokers
     *               The candidate brokers list.
     * @param context
     *               The context includes a variety of information needed for selection.
     */
    Optional<String> select(List<String> brokers, LoadManagerContext context);

}
