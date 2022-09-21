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

import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import org.apache.pulsar.broker.PulsarServerException;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.lookup.LookupResult;
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
    void start() throws PulsarServerException;

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
    Optional<String> discover(ServiceUnitId serviceUnit);


    /**
     * The incoming bundle selects the appropriate broker through strategies.
     *
     * @param serviceUnit Bundle.
     * @return Simple resource.
     */
    CompletableFuture<Optional<LookupResult>> assign(Optional<ServiceUnitId> topic, ServiceUnitId serviceUnit);

    /**
     * Stop the load manager.
     */
    void stop() throws PulsarServerException;
}
