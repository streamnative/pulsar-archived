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
package org.apache.bookkeeper.mledger.impl;

import static org.apache.bookkeeper.mledger.util.SafeRun.safeRun;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.api.ReadHandle;
import org.apache.bookkeeper.common.util.OrderedExecutor;
import org.apache.bookkeeper.common.util.OrderedScheduler;
import org.apache.bookkeeper.mledger.AsyncCallbacks;
import org.apache.bookkeeper.mledger.LedgerOffloaderV2;
import org.apache.bookkeeper.mledger.ManagedCursor;
import org.apache.bookkeeper.mledger.ManagedLedgerConfig;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.proto.MLDataFormats.ManagedLedgerInfo.LedgerInfo;
import org.apache.pulsar.common.api.proto.CommandSubscribe.InitialPosition;

@Slf4j
public class ManagedLedgerTieredImpl extends ManagedLedgerImpl {
    @Override
    protected synchronized void initialize(ManagedLedgerInitializeLedgerCallback callback, Object ctx) {
        final ManagedLedgerInitializeLedgerCallback wrappedCalllback = new ManagedLedgerInitializeLedgerCallback() {
            @Override
            public void initializeComplete() {
                try {
                    initializeOffloadCursor();
                } catch (ManagedLedgerException | InterruptedException e) {
                    log.error("initialize offload cursor failed", e);
                    callback.initializeFailed(new ManagedLedgerException(e));
                }
                callback.initializeComplete();
            }

            @Override
            public void initializeFailed(ManagedLedgerException e) {
                callback.initializeFailed(e);
            }
        };
        super.initialize(wrappedCalllback, ctx);
    }

    static final String offloadCursorName = "_offload_cursor";
    volatile ManagedCursor offloadCursor;

    @Override
    public CompletableFuture<ReadHandle> readOffloaded(Long ledgerId, LedgerInfo info) {
        return ((LedgerOffloaderV2) this.config.getLedgerOffloader()).readOffloaded(name, ledgerId, new HashMap<>());
    }

    @Override
    public boolean isOffloadCompleted(Long ledgerId, LedgerInfo info) {
        return ledgerId <= offloadCursor.getMarkDeletedPosition().getLedgerId();
    }

    @Override
    protected boolean isOffloadedNeedsDelete(Long ledgerId, LedgerInfo ledgerInfo) {
        //offloaded data will be deleted in bk instantly because no place to store offloaded time
        return ledgerId <= offloadCursor.getMarkDeletedPosition().getLedgerId();
    }

    public ManagedLedgerTieredImpl(ManagedLedgerFactoryImpl factory, BookKeeper bookKeeper,
                                   MetaStore store, ManagedLedgerConfig config,
                                   OrderedScheduler scheduledExecutor,
                                   OrderedExecutor orderedExecutor, String name) {
        super(factory, bookKeeper, store, config, scheduledExecutor, orderedExecutor, name);
    }

    public ManagedLedgerTieredImpl(ManagedLedgerFactoryImpl factory, BookKeeper bookKeeper,
                                   MetaStore store, ManagedLedgerConfig config,
                                   OrderedScheduler scheduledExecutor,
                                   OrderedExecutor orderedExecutor, String name,
                                   Supplier<Boolean> mlOwnershipChecker) {
        super(factory, bookKeeper, store, config, scheduledExecutor, orderedExecutor, name, mlOwnershipChecker);
    }

    public void initializeOffloadCursor() throws ManagedLedgerException, InterruptedException {
        final ManagedCursor cursor = cursors.get(offloadCursorName);
        if (cursor == null) {
            offloadCursor = this.openCursor(offloadCursorName, InitialPosition.Earliest);
        } else {
            offloadCursor = cursor;
        }
        offloadCursor.setActive();
    }

    @Override
    public void asyncOffloadPrefix(Position pos, AsyncCallbacks.OffloadCallback callback, Object ctx) {
        if (!offloadMutex.tryLock()) {
            callback.offloadFailed(new ManagedLedgerException("offloading in progress"), null);
        }
        final LinkedList<Long> ledgersToOffload = new LinkedList<>(
                ledgers.subMap(offloadCursor.getMarkDeletedPosition().getLedgerId() + 1,
                        Math.min(currentLedger.getId(), pos.getLedgerId())).keySet());
        offloadLoop(ledgersToOffload, callback);
    }

    private void offloadLoop(LinkedList<Long> ledgersToOffload, AsyncCallbacks.OffloadCallback callback) {
        if (ledgersToOffload.isEmpty()) {
            callback.offloadComplete(offloadCursor.getMarkDeletedPosition(), null);
            offloadMutex.unlock();
            return;
        }
        final Long head = ledgersToOffload.pop();
        final CompletableFuture<LedgerOffloaderV2.OffloadResultV2> future = ((LedgerOffloaderV2) config
                .getLedgerOffloader())
                .offloadV2(offloadCursor, name,
                        new LedgerOffloaderV2.OffloadOption(head, LedgerOffloaderV2.OffloadMethod.LEGER_BASED,
                                new HashMap<>()));
        future.whenComplete((result, ex) -> {
            //should always equal in written position
            assert result.completePosition == result.wittenPosition;
            if (ex != null) {
                callback.offloadFailed(new ManagedLedgerException("offload failed", ex), null);
                offloadMutex.unlock();
                return;
            }

            try {
                offloadCursor.markDelete(result.completePosition);
            } catch (InterruptedException | ManagedLedgerException e) {
                offloadMutex.unlock();
                callback.offloadFailed(new ManagedLedgerException("mark delete failed", e), null);
                return;
            }

            offloadLoop(ledgersToOffload, callback);
        });
    }

    @Override
    protected void maybeOffload(CompletableFuture<Position> finalPromise) {
        if (!offloadMutex.tryLock()) {
            scheduledExecutor.schedule(safeRun(() -> maybeOffload(finalPromise)),
                    100, TimeUnit.MILLISECONDS);
        } else {
            long threshold = config.getLedgerOffloader().getOffloadPolicies().getManagedLedgerOffloadThresholdInBytes();
            long summedSize = 0;
            LinkedList<Long> ledgersToOffload = new LinkedList<>();

            //current ledger will not be offloaded because it's size is zero
            for (Map.Entry<Long, LedgerInfo> idLedgerInfo : ledgers
                    .descendingMap().entrySet()) {
                final Long ledgerId = idLedgerInfo.getKey();
                if (ledgerId <= offloadCursor.getMarkDeletedPosition().getLedgerId()) {
                    //previous ledgers should be all offloaded
                    break;
                }
                long size = idLedgerInfo.getValue().getSize();
                summedSize += size;
                if (summedSize > threshold) {
                    ledgersToOffload.addFirst(ledgerId);
                }
            }

            if (ledgersToOffload.size() > 0) {
                log.info("begin offload for {}, ledgers {}, size in bytes {}", name, ledgersToOffload, summedSize);
            }

            offloadLoop(ledgersToOffload, new AsyncCallbacks.OffloadCallback() {
                @Override
                public void offloadComplete(Position pos, Object ctx) {
                    finalPromise.complete(pos);
                }

                @Override
                public void offloadFailed(ManagedLedgerException exception, Object ctx) {
                    finalPromise.completeExceptionally(exception);
                }
            });
        }
    }
}
