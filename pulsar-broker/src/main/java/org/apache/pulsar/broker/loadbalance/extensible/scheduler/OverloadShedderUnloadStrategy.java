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

import java.util.Map;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.mutable.MutableBoolean;
import org.apache.commons.lang3.mutable.MutableDouble;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.loadbalance.extensible.BaseLoadManagerContext;
import org.apache.pulsar.broker.namespace.NamespaceService;
import org.apache.pulsar.common.naming.NamespaceBundle;
import org.apache.pulsar.policies.data.loadbalancer.BundleData;
import org.apache.pulsar.policies.data.loadbalancer.TimeAverageMessageData;

/**
 * The overload shedder unload strategy.
 * {@link org.apache.pulsar.broker.loadbalance.impl.OverloadShedder}.
 */
@Slf4j
public class OverloadShedderUnloadStrategy implements NamespaceUnloadStrategy {

    private final Multimap<String, String> selectedBundlesCache = ArrayListMultimap.create();

    private static final double ADDITIONAL_THRESHOLD_PERCENT_MARGIN = 0.05;

    @Override
    public Multimap<String, String> findBundlesForUnloading(BaseLoadManagerContext context,
                                                            Map<String, Long> recentlyUnloadedBundles) {

        selectedBundlesCache.clear();
        ServiceConfiguration conf = context.brokerConfiguration();
        final double overloadThreshold = conf.getLoadBalancerBrokerOverloadedThresholdPercentage() / 100.0;

        // Check every broker and select
        context.brokerLoadDataStore().forEach((broker, brokerLoadData) -> {

            final double currentUsage = brokerLoadData.getMaxResourceUsage();
            if (currentUsage < overloadThreshold) {
                if (log.isDebugEnabled()) {
                    log.debug("[{}] Broker is not overloaded, ignoring at this point ({})", broker,
                            brokerLoadData.printResourceUsage());
                }
                return;
            }

            // We want to offload enough traffic such that this broker will go below the overload threshold
            // Also, add a small margin so that this broker won't be very close to the threshold edge.
            double percentOfTrafficToOffload = currentUsage - overloadThreshold + ADDITIONAL_THRESHOLD_PERCENT_MARGIN;
            double brokerCurrentThroughput = brokerLoadData.getMsgThroughputIn() + brokerLoadData.getMsgThroughputOut();

            double minimumThroughputToOffload = brokerCurrentThroughput * percentOfTrafficToOffload;

            log.info("Attempting to shed load on {}, which has resource usage {}% above threshold {}%"
                            + " -- Offloading at least {} MByte/s of traffic ({})",
                    broker, 100 * currentUsage, 100 * overloadThreshold, minimumThroughputToOffload / 1024 / 1024,
                    brokerLoadData.printResourceUsage());

            MutableDouble trafficMarkedToOffload = new MutableDouble(0);
            MutableBoolean atLeastOneBundleSelected = new MutableBoolean(false);

            if (brokerLoadData.getBundles().size() > 1) {
                // Sort bundles by throughput, then pick the biggest N which combined
                // make up for at least the minimum throughput to offload

                context.bundleLoadDataStore()
                        .entrySet()
                        .stream()
                        .filter(e -> {
                            // Make sure filter out the system service namespace.
                            return !NamespaceService.isSystemServiceNamespace(
                                NamespaceBundle.getBundleNamespace(e.getKey()));
                        })
                        .filter(e -> brokerLoadData.getBundles().contains(e.getKey()))
                        .map((e) -> {
                            // Map to throughput value
                            // Consider short-term byte rate to address system resource burden
                            String bundle = e.getKey();
                            BundleData bundleData = e.getValue();
                            TimeAverageMessageData shortTermData = bundleData.getShortTermData();
                            double throughput = shortTermData.getMsgThroughputIn() + shortTermData
                                    .getMsgThroughputOut();
                            return Pair.of(bundle, throughput);
                        })
                        .filter(e -> {
                            // Only consider bundles that were not already unloaded recently
                            return !recentlyUnloadedBundles.containsKey(e.getLeft());
                        })
                        .filter(e ->
                                brokerLoadData.getBundles().contains(e.getLeft())
                        )
                        .sorted((e1, e2) -> {
                            // Sort by throughput in reverse order
                            return Double.compare(e2.getRight(), e1.getRight());
                        })
                        .forEach(e -> {
                            if (trafficMarkedToOffload.doubleValue() < minimumThroughputToOffload
                                    || atLeastOneBundleSelected.isFalse()) {
                                selectedBundlesCache.put(broker, e.getLeft());
                                trafficMarkedToOffload.add(e.getRight());
                                atLeastOneBundleSelected.setTrue();
                            }
                        });
            } else if (brokerLoadData.getBundles().size() == 1) {
                log.warn(
                        "HIGH USAGE WARNING : Sole namespace bundle {} is overloading broker {}. "
                                + "No Load Shedding will be done on this broker",
                        brokerLoadData.getBundles().iterator().next(), broker);
            } else {
                log.warn("Broker {} is overloaded despite having no bundles", broker);
            }

        });

        return selectedBundlesCache;
    }
}
