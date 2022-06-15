/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pulsar.broker.loadbalance.extensible;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

import org.apache.pulsar.broker.PulsarServerException;

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

    String getLookupServiceAddress();

    /**
     * Get available brokers.
     */
    List<String> getAvailableBrokers();

    /**
     * Async get available brokers.
     */
    CompletableFuture<List<String>> getAvailableBrokersAsync();

    /**
     * Fetch local-broker data from load-manager broker cache.
     *
     * @param broker The load-balancer path.
     */
    Optional<BrokerLookupData> lookup(String broker);

    /**
     * For each the broker lookup data.
     * The key is lookupServiceAddress
     */
    void forEach(BiConsumer<String, BrokerLookupData> action);

    /**
     * Listen the broker register change.
     */
    void listen(Consumer<String> listener);

    /**
     * Close the broker registry.
     */
    void close() throws Exception;
}
