package org.apache.pulsar.broker.loadbalance.test;

import org.apache.pulsar.broker.ServiceConfiguration;

/**
 * Users can extend the Context class and impl the BrokerDiscovery itself.
 */
public interface LoadManagerContext {

    /**
     * The broker configuration.
     */
    ServiceConfiguration brokerConfiguration();
}
