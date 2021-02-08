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

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import org.apache.bookkeeper.client.api.ReadHandle;

public interface LedgerOffloaderV2 {

    enum OffloadMethod {
        LEGER_BASED,
        STREAMING,
    }

    class OffloadOption {
        //only valid when using ledger-based offload
        final public Long ledgerId;
        final public OffloadMethod offloadMethod;
        //threshold, options etc
        final public Map<String, Object> extraMetadata;

        public OffloadOption(Long ledgerId, OffloadMethod offloadMethod,
                             Map<String, Object> extraMetadata) {
            this.ledgerId = ledgerId;
            this.offloadMethod = offloadMethod;
            this.extraMetadata = extraMetadata;
        }
    }

    class OffloadResultV2 {
        //Data written to tiered storage but not ready to serve because
        //A full ledger is not offloaded yet
        final public Position wittenPosition;
        //Full written ledger data, always point to the last entry of a ledger
        final public Position completePosition;

        public OffloadResultV2(Position wittenPosition, Position completePosition) {
            this.wittenPosition = wittenPosition;
            this.completePosition = completePosition;
        }
    }

    default CompletableFuture<OffloadResultV2> offloadV2(ManagedCursor cursor,
                                                         String managedLedgerName,
                                                         OffloadOption option) {
        throw new UnsupportedOperationException(
                String.format(
                        "Your offloader %s not support offloadALedger,"
                                + " implement it or use another one that support it",
                        this.getClass()));
    }

    default CompletableFuture<ReadHandle> readOffloaded(
            String managedLedgerName,
            long ledgerId,
            Map<String, String> extraMetadata) {
        throw new UnsupportedOperationException(
                "You offloaded not implemented readOffloaded by topic name and ledger id"
                        + " try use others instead: " + this.getClass());
    }

    default CompletableFuture<ReadHandle> deleteOffloaded(
            String managedLedgerName,
            long ledgerId,
            Map<String, String> extraMetadata) {
        throw new UnsupportedOperationException(
                "You offloaded not implemented delete offloaded by topic name and ledger id"
                        + " try use others instead: " + this.getClass());
    }

}
