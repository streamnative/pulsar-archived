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

import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.common.util.OrderedExecutor;
import org.apache.bookkeeper.common.util.OrderedScheduler;
import org.apache.bookkeeper.mledger.AsyncCallbacks;
import org.apache.bookkeeper.mledger.ManagedCursor;
import org.apache.bookkeeper.mledger.ManagedLedgerConfig;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.Position;
import org.apache.pulsar.common.api.proto.CommandSubscribe.InitialPosition;

public class ManagedLedgerTieredImpl extends ManagedLedgerImpl {

    //TODO offload but not change meta
    //TODO rewrite check offloaded logic
    //TODO change read logic in cursor
    static final String offloadCursorName = "_offload_cursor";
    volatile ManagedCursor offloadCursor;

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
    }

    @Override
    public void asyncOffloadPrefix(Position pos, AsyncCallbacks.OffloadCallback callback, Object ctx) {
        //TODO rewrite
        super.asyncOffloadPrefix(pos, callback, ctx);
    }

    @Override
    protected void maybeOffloadInBackground(CompletableFuture<PositionImpl> promise) {
        //TODO rewrite
        super.maybeOffloadInBackground(promise);
    }
}
