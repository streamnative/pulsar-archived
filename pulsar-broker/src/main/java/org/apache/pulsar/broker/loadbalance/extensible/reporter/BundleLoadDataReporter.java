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
package org.apache.pulsar.broker.loadbalance.extensible.reporter;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.pulsar.broker.loadbalance.extensible.data.BrokerLoadData;
import org.apache.pulsar.broker.loadbalance.extensible.data.LoadDataStore;
import org.apache.pulsar.policies.data.loadbalancer.BundleData;
import org.apache.pulsar.policies.data.loadbalancer.NamespaceBundleStats;

public class BundleLoadDataReporter extends AbstractLoadDataReporter<Map<String, BundleData>> {

    private final LoadDataStore<BundleData> bundleLoadDataStore;

    public final LoadDataStore<BrokerLoadData> brokerLoadDataStore;

    final Map<String, BundleData> bundleLoadDataMap;

    public BundleLoadDataReporter(LoadDataStore<BundleData> bundleLoadDataStore,
                                  LoadDataStore<BrokerLoadData> brokerLoadDataStore) {
        this.bundleLoadDataStore = bundleLoadDataStore;
        this.brokerLoadDataStore = brokerLoadDataStore;
        this.bundleLoadDataMap = new ConcurrentHashMap<>();
    }

    @Override
    public Map<String, BundleData> generateLoadData() {
        this.brokerLoadDataStore.forEach((broker, brokerLoadData) -> {
            Map<String, NamespaceBundleStats> statsMap = brokerLoadData.getLastStats();
            statsMap.forEach((bundle, stats) -> {
                BundleData currentBundleData =
                        this.bundleLoadDataMap.getOrDefault(bundle,
                                bundleLoadDataStore.get(bundle).orElse(BundleData.newDefaultBundleData()));
                currentBundleData.update(stats);
                this.bundleLoadDataMap.put(bundle, currentBundleData);
            });
            // TODO: delete inactive bundles.
        });
        return this.bundleLoadDataMap;
    }

    @Override
    public void start() {

    }

    @Override
    public void flush() {

    }

    @Override
    public void close() throws Exception {

    }
}
