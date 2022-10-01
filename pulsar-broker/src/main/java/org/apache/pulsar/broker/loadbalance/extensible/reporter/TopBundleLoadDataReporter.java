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

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.loadbalance.extensible.data.LoadDataStore;
import org.apache.pulsar.broker.loadbalance.extensible.data.TopBundlesLoadData;
import org.apache.pulsar.common.naming.NamespaceName;

@Slf4j
public class TopBundleLoadDataReporter extends AbstractLoadDataReporter<TopBundlesLoadData> {

    private final PulsarService pulsar;

    private final String lookupServiceAddress;

    private final LoadDataStore<TopBundlesLoadData> bundleLoadDataStore;

    public TopBundleLoadDataReporter(PulsarService pulsar,
                                     String lookupServiceAddress,
                                     LoadDataStore<TopBundlesLoadData> bundleLoadDataStore) {
        this.pulsar = pulsar;
        this.lookupServiceAddress = lookupServiceAddress;
        this.bundleLoadDataStore = bundleLoadDataStore;
    }

    @Override
    public TopBundlesLoadData generateLoadData() {

        String pulsarFunctionsNamespace = pulsar.getWorkerServiceOpt().isEmpty()
                ? pulsar.getWorkerServiceOpt().get().getWorkerConfig().getPulsarFunctionsNamespace()
                : "public/functions";
        var bundleStats = this.getBundleStats(pulsar);
        List<TopBundlesLoadData.BundleLoadData> filteredBundleStats = null;
        synchronized (bundleStats) {
            filteredBundleStats = bundleStats.entrySet().stream()
                    //skip system namespace
                    .filter(e -> !e.getKey().startsWith(NamespaceName.SYSTEM_NAMESPACE.toString()))
                    .filter(e -> !e.getKey().startsWith(pulsarFunctionsNamespace))
                    .map(e -> new TopBundlesLoadData.BundleLoadData(e.getKey(), e.getValue()))
                    .collect(Collectors.toList());
            // TODO: Make it configurable.
        }
        return TopBundlesLoadData.of(filteredBundleStats, 5);
    }

    @Override
    public CompletableFuture<Void> reportAsync(boolean force) {
        return this.bundleLoadDataStore.pushAsync(lookupServiceAddress, this.generateLoadData());
    }
}
