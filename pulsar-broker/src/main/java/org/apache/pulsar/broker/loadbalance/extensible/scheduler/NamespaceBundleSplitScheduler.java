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
package org.apache.pulsar.broker.loadbalance.extensible.scheduler;

import java.util.Set;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.loadbalance.extensible.BaseLoadManagerContext;
import org.apache.pulsar.broker.loadbalance.impl.LoadManagerShared;
import org.apache.pulsar.common.naming.NamespaceBundleFactory;
import org.apache.pulsar.common.naming.NamespaceName;

/**
 * Split bundle scheduler.
 */
@Slf4j
public class NamespaceBundleSplitScheduler implements LoadManagerScheduler {

    private final PulsarService pulsar;

    private final BaseLoadManagerContext context;

    private final ServiceConfiguration conf;

    private final NamespaceBundleSplitStrategy bundleSplitStrategy;


    public NamespaceBundleSplitScheduler(PulsarService pulsar,
                                         BaseLoadManagerContext context) {
        this.pulsar = pulsar;
        this.context = context;
        this.conf = context.brokerConfiguration();
//        this.bundleSplitStrategy = new DefaultNamespaceBundleSplitStrategyImpl();
        this.bundleSplitStrategy = null;
    }


    @Override
    public void execute() {
        if (!this.isLoadBalancerAutoBundleSplitEnabled() || !this.isLeader()) {
            return;
        }
        final boolean unloadSplitBundles =
                pulsar.getConfiguration().isLoadBalancerAutoUnloadSplitBundlesEnabled();
        synchronized (bundleSplitStrategy) {
            final Set<String> bundlesToBeSplit =
                    bundleSplitStrategy.findBundlesToSplit(context, pulsar.getNamespaceService());
            NamespaceBundleFactory namespaceBundleFactory =
                    pulsar.getNamespaceService().getNamespaceBundleFactory();
            for (String bundleName : bundlesToBeSplit) {
                try {
                    final String namespaceName = LoadManagerShared.getNamespaceNameFromBundleName(bundleName);
                    final String bundleRange = LoadManagerShared.getBundleRangeFromBundleName(bundleName);
                    if (!namespaceBundleFactory
                            .canSplitBundle(namespaceBundleFactory.getBundle(namespaceName, bundleRange))) {
                        continue;
                    }

                    // Make sure the same bundle is not selected again.
                    context.topBundleLoadDataStore().remove(bundleName);
                    // Clear namespace bundle-cache
                    namespaceBundleFactory
                            .invalidateBundleCache(NamespaceName.get(namespaceName));

                    log.info("Load-manager splitting bundle {} and unloading {}", bundleName, unloadSplitBundles);

                    pulsar.getAdminClient().namespaces().splitNamespaceBundle(namespaceName, bundleRange,
                            unloadSplitBundles, null);

                    log.info("Successfully split namespace bundle {}", bundleName);
                } catch (Exception e) {
                    log.error("Failed to split namespace bundle {}", bundleName, e);
                }
            }
        }
    }

    @Override
    public void start() {

    }

    @Override
    public void close() {

    }

    private boolean isLoadBalancerAutoBundleSplitEnabled() {
        return conf.isLoadBalancerAutoBundleSplitEnabled();
    }

    private boolean isLeader() {
        return pulsar.getLeaderElectionService() != null && pulsar.getLeaderElectionService().isLeader();
    }
}
