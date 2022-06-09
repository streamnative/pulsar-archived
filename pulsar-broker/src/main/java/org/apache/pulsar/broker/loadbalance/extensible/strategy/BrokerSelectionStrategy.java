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
package org.apache.pulsar.broker.loadbalance.extensible.strategy;

import org.apache.pulsar.broker.loadbalance.extensible.LoadManagerContext;

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
