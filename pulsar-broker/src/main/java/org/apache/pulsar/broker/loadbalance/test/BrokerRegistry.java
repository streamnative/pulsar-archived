package org.apache.pulsar.broker.loadbalance.test;

import org.apache.pulsar.broker.PulsarServerException;

import java.util.Set;
import java.util.concurrent.CompletableFuture;

/**
 * Responsible for registering the current Broker lookup info to the distributed store (Zookeeper) for broker discovery.
 */
public interface BrokerRegistry {

    void start();

    /**
     * Register local broker to metadata store.
     */
    void register();

    /**
     * Unregister the broker.
     *
     * Same as {@link org.apache.pulsar.broker.loadbalance.ModularLoadManager#disableBroker()}
     */
    void unregister() throws PulsarServerException;

    /**
     * Get available brokers.
     */
    Set<String> getAvailableBrokers();

    /**
     * Async get available brokers.
     */
    CompletableFuture<Set<String>> getAvailableBrokersAsync();

    /**
     * Fetch local-broker data from load-manager broker cache.
     *
     * @param broker The load-balancer path.
     */
    BrokerLookupData lookup(String broker);

    void close() throws Exception;
}
