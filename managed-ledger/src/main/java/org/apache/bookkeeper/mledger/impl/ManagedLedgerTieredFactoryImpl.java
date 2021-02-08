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
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.conf.ClientConfiguration;
import org.apache.bookkeeper.mledger.ManagedLedgerConfig;
import org.apache.bookkeeper.mledger.ManagedLedgerFactoryConfig;
import org.apache.bookkeeper.stats.StatsLogger;
import org.apache.pulsar.common.policies.data.EnsemblePlacementPolicyConfig;
import org.apache.zookeeper.ZooKeeper;

public class ManagedLedgerTieredFactoryImpl extends ManagedLedgerFactoryImpl {
    public ManagedLedgerTieredFactoryImpl(ClientConfiguration bkClientConfiguration,
                                          String zkConnection) throws Exception {
        super(bkClientConfiguration, zkConnection);
    }

    public ManagedLedgerTieredFactoryImpl(ClientConfiguration bkClientConfiguration,
                                          ManagedLedgerFactoryConfig config) throws
            Exception {
        super(bkClientConfiguration, config);
    }

    public ManagedLedgerTieredFactoryImpl(BookKeeper bookKeeper,
                                          ZooKeeper zooKeeper) throws Exception {
        super(bookKeeper, zooKeeper);
    }

    public ManagedLedgerTieredFactoryImpl(BookKeeper bookKeeper, ZooKeeper zooKeeper,
                                          ManagedLedgerFactoryConfig config) throws Exception {
        super(bookKeeper, zooKeeper, config);
    }

    public ManagedLedgerTieredFactoryImpl(
            BookkeeperFactoryForCustomEnsemblePlacementPolicy bookKeeperGroupFactory,
            ZooKeeper zooKeeper, ManagedLedgerFactoryConfig config) throws Exception {
        super(bookKeeperGroupFactory, zooKeeper, config);
    }

    public ManagedLedgerTieredFactoryImpl(
            BookkeeperFactoryForCustomEnsemblePlacementPolicy bookKeeperGroupFactory,
            ZooKeeper zooKeeper, ManagedLedgerFactoryConfig config,
            StatsLogger statsLogger) throws Exception {
        super(bookKeeperGroupFactory, zooKeeper, config, statsLogger);
    }

    @Override
    protected ManagedLedgerImpl createNewManagedLedger(String name, ManagedLedgerConfig config,
                                                       Supplier<Boolean> mlOwnershipChecker) {
        return new ManagedLedgerTieredImpl(this,
                bookkeeperFactory.get(
                        new EnsemblePlacementPolicyConfig(config.getBookKeeperEnsemblePlacementPolicyClassName(),
                                config.getBookKeeperEnsemblePlacementPolicyProperties())),
                store, config, scheduledExecutor,
                orderedExecutor, name, mlOwnershipChecker);
    }
}
