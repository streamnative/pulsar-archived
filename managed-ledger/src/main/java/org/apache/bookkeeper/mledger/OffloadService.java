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
package org.apache.bookkeeper.mledger;

import java.util.concurrent.CompletableFuture;
import org.apache.bookkeeper.client.api.ReadHandle;
import org.apache.bookkeeper.common.util.OrderedExecutor;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.common.policies.data.OffloadPoliciesImpl;

/**
 * The offload service is used for offload the message from a topic.
 * Each topic will have individual offload service.
 */
public interface OffloadService {

    /**
     * Initialize the offload service.
     * @return
     */
    default CompletableFuture<Void> initialize() {
        return CompletableFuture.completedFuture(null);
    };

    /**
     * Trigger offload process for the topic. The offload process can keep running or stop in specific condition.
     * @param topic
     * @return
     */
    default CompletableFuture<Void> offload(String topic) {
        return CompletableFuture.completedFuture(null);
    }

    /**
     * Close the offload service.
     * @return
     */
    default CompletableFuture<Void> closeAsync() {
        return CompletableFuture.completedFuture(null);
    };

    default String getOffloadDriverName() {
        return "default";
    };

    /**
     * Get offload policies of this LedgerOffloader
     *
     * @return offload policies
     */
    default OffloadPoliciesImpl getOffloadPolicies() {
        return null;
    };

    /**
     * Get related ledger's ReadHandle.
     * @param ledgerId
     * @return
     */
    default CompletableFuture<ReadHandle> readOffloaded(long ledgerId, String managedLedgerName) {
        return CompletableFuture.completedFuture(null);
    };

    /**
     * Get related ledger's ReadHandle.
     * @param ledgerId
     * @param executor
     * @param conf
     * @return
     */
    default CompletableFuture<ReadHandle> readOffloaded(long ledgerId,
                                                        String managedLedgerName,
                                                        OrderedExecutor executor, ServiceConfiguration conf) {
        return CompletableFuture.completedFuture(null);
    };

    default void close() {
    };
}

