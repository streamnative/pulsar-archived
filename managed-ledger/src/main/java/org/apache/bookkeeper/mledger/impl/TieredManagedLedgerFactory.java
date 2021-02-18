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

import java.util.function.Supplier;
import org.apache.bookkeeper.mledger.AsyncCallbacks;
import org.apache.bookkeeper.mledger.AsyncCallbacks.DeleteLedgerCallback;
import org.apache.bookkeeper.mledger.AsyncCallbacks.ManagedLedgerInfoCallback;
import org.apache.bookkeeper.mledger.AsyncCallbacks.OpenLedgerCallback;
import org.apache.bookkeeper.mledger.ManagedLedger;
import org.apache.bookkeeper.mledger.ManagedLedgerConfig;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.ManagedLedgerFactoryConfig;
import org.apache.bookkeeper.mledger.ManagedLedgerInfo;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.ReadOnlyCursor;
import org.apache.bookkeeper.stats.StatsLogger;
import org.apache.zookeeper.ZooKeeper;

public class TieredManagedLedgerFactory extends ManagedLedgerFactoryImpl {
    public TieredManagedLedgerFactory(
            BookkeeperFactoryForCustomEnsemblePlacementPolicy bookKeeperGroupFactory,
            ZooKeeper zooKeeper, ManagedLedgerFactoryConfig config,
            StatsLogger statsLogger) throws Exception {
        super(bookKeeperGroupFactory, zooKeeper, config, statsLogger);
    }

    @Override
    public ManagedLedger open(String s) throws InterruptedException, ManagedLedgerException {
        return null;
    }

    @Override
    public ManagedLedger open(String s, ManagedLedgerConfig managedLedgerConfig)
            throws InterruptedException, ManagedLedgerException {

        // TODO: immplement the logic

        ManagedLedgerConfig originalConfig = managedLedgerConfig;

        // TODO: make a copy of the managed ledger config which disables
        ManagedLedgerConfig innerConfig = managedLedgerConfig;


        ManagedLedger ledger = open(s, innerConfig);

//        return new TieredManagedLedger(originalConfig, ledger);
        return null;
    }

    @Override
    public void asyncOpen(String s, OpenLedgerCallback openLedgerCallback, Object o) {

    }

    @Override
    public void asyncOpen(String s, ManagedLedgerConfig managedLedgerConfig, OpenLedgerCallback openLedgerCallback,
                          Supplier<Boolean> supplier, Object o) {

    }

    @Override
    public ReadOnlyCursor openReadOnlyCursor(String s, Position position,
                                             ManagedLedgerConfig managedLedgerConfig) throws InterruptedException,
            ManagedLedgerException {
        return null;
    }

    @Override
    public void asyncOpenReadOnlyCursor(String s, Position position, ManagedLedgerConfig managedLedgerConfig,
                                        AsyncCallbacks.OpenReadOnlyCursorCallback openReadOnlyCursorCallback,
                                        Object o) {

    }

    @Override
    public ManagedLedgerInfo getManagedLedgerInfo(String s) throws InterruptedException, ManagedLedgerException {
        return null;
    }

    @Override
    public void asyncGetManagedLedgerInfo(String s, ManagedLedgerInfoCallback managedLedgerInfoCallback, Object o) {

    }

    @Override
    public void delete(String s) throws InterruptedException, ManagedLedgerException {

    }

    @Override
    public void asyncDelete(String s, DeleteLedgerCallback deleteLedgerCallback, Object o) {

    }

    @Override
    public void shutdown() throws InterruptedException, ManagedLedgerException {

    }
}
