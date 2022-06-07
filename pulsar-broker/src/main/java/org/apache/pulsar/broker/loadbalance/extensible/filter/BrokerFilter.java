package org.apache.pulsar.broker.loadbalance.extensible.filter;

import java.util.List;
import org.apache.pulsar.broker.loadbalance.extensible.LoadManagerContext;

/**
 * Filter out unqualified Brokers, which are not entered into LoadBalancer for decision-making.
 */
public interface BrokerFilter {

    String name();

    void filter(List<String> brokers, LoadManagerContext context);

}
