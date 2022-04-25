package org.apache.pulsar.broker.loadbalance.test;

import java.util.Optional;

import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.loadbalance.ResourceUnit;
import org.apache.pulsar.common.naming.ServiceUnitId;

/**
 * Find the appropriate broker for bundle through different load balancer Implementation.
 */
public interface BrokerDiscovery {

    /**
     * Start the broker discovery.
     *
     * 1. register self broker to ZNode
     */
    void start();

    /**
     * Initialize this broker discovery using the given pulsar service.
     */
    void initialize(PulsarService pulsar);

    /**
     * The incoming bundle selects the appropriate broker through strategies.
     *
     * @param serviceUnit Bundle.
     * @return Simple resource.
     */
    Optional<ResourceUnit> discover(ServiceUnitId serviceUnit);

    void stop();
}
