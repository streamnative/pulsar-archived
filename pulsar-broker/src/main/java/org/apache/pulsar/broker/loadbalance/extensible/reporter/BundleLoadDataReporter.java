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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.loadbalance.extensible.data.LoadDataStore;
import org.apache.pulsar.common.util.FutureUtil;
import org.apache.pulsar.policies.data.loadbalancer.BundleData;
import org.apache.pulsar.policies.data.loadbalancer.NamespaceBundleStats;

@Slf4j
public class BundleLoadDataReporter extends AbstractLoadDataReporter<Map<String, BundleData>> {

    private final PulsarService pulsar;

    private final LoadDataStore<BundleData> bundleLoadDataStore;

    private final Map<String, BundleData> bundleLoadDataMap;

    public BundleLoadDataReporter(PulsarService pulsar, LoadDataStore<BundleData> bundleLoadDataStore) {
        this.pulsar = pulsar;
        this.bundleLoadDataStore = bundleLoadDataStore;
        this.bundleLoadDataMap = new ConcurrentHashMap<>();
    }

    @Override
    public Map<String, BundleData> generateLoadData() {
        this.bundleLoadDataMap.clear();
        Map<String, NamespaceBundleStats> statsMap = this.getBundleStats(pulsar);
        statsMap.forEach((bundle, stats) -> {
            BundleData currentBundleData =
                    this.bundleLoadDataMap.getOrDefault(bundle,
                            bundleLoadDataStore.get(bundle).orElse(BundleData.newDefaultBundleData()));
            currentBundleData.update(stats);
            this.bundleLoadDataMap.put(bundle, currentBundleData);
        });
        // TODO: Delete inactive bundle.
        return this.bundleLoadDataMap;
    }

    @Override
    public CompletableFuture<Void> reportAsync(boolean force) {
        List<CompletableFuture<Void>> futureList = new ArrayList<>();
        this.generateLoadData().forEach((bundle, bundleData) -> {
            futureList.add(this.bundleLoadDataStore.pushAsync(bundle, bundleData));
        });
        return FutureUtil.waitForAll(futureList).thenAccept(__ -> {}).exceptionally(ex -> {
            log.error("Report the bundle load data failed.", ex);
            return null;
        }).thenAccept(__ -> {});
    }
}
