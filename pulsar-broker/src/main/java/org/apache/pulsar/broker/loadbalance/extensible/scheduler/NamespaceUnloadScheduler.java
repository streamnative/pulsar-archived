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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.loadbalance.extensible.BaseLoadManagerContext;
import org.apache.pulsar.broker.loadbalance.extensible.channel.BundleStateChannel;
import org.apache.pulsar.broker.loadbalance.extensible.data.Unload;

/**
 * As a leader, it will select bundles for the namespace service to unload
 * so that they may be reassigned to new brokers.
 */
@Slf4j
public class NamespaceUnloadScheduler implements LoadManagerScheduler {

    private final List<NamespaceUnloadStrategy> namespaceUnloadStrategyPipeline;

    private final PulsarService pulsar;

    private final BaseLoadManagerContext context;

    private final ServiceConfiguration configuration;

    private final Map<String, Long> recentlyUnloadedBundles;

    private volatile ScheduledFuture<?> loadSheddingTask;

    private final BundleStateChannel bundleStateChannel;

    public NamespaceUnloadScheduler(PulsarService pulsar,
                                    BaseLoadManagerContext context,
                                    BundleStateChannel bundleStateChannel) {
        this.namespaceUnloadStrategyPipeline = new ArrayList<>();
        this.namespaceUnloadStrategyPipeline.add(new ThresholdShedder());
        this.recentlyUnloadedBundles = new HashMap<>();
        this.pulsar = pulsar;
        this.context = context;
        this.configuration = context.brokerConfiguration();
        this.bundleStateChannel = bundleStateChannel;
    }

    @Override
    public void execute() {
        if (!(configuration.isLoadBalancerEnabled()
                && configuration.isLoadBalancerSheddingEnabled())
                || !this.isLeader()) {
            return;
        }
        if (context.brokerRegistry().getAvailableBrokers().size() <= 1) {
            log.info("Only 1 broker available: no load shedding will be performed");
            return;
        }
        // Remove bundles who have been unloaded for longer than the grace period from the recently unloaded map.
        final long timeout = System.currentTimeMillis()
                - TimeUnit.MINUTES.toMillis(configuration.getLoadBalancerSheddingGracePeriodMinutes());
        recentlyUnloadedBundles.keySet().removeIf(e -> recentlyUnloadedBundles.get(e) < timeout);

        for (NamespaceUnloadStrategy strategy : namespaceUnloadStrategyPipeline) {
            final List<Unload> bundlesToUnload =
                    strategy.findBundlesForUnloading(context, recentlyUnloadedBundles);

            bundlesToUnload.forEach(data -> {
                log.info("[{}] Unloading bundle: {}",
                        strategy.getClass().getSimpleName(), data);
                // TODO: wait for the complete
                bundleStateChannel.unloadBundle(data);
                recentlyUnloadedBundles.put(data.getBundle(), System.currentTimeMillis());
            });
        }

    }

    @Override
    public void start() {
        ScheduledExecutorService loadManagerExecutor = this.pulsar.getLoadManagerExecutor();
        long loadSheddingInterval = TimeUnit.MINUTES
                .toMillis(configuration.getLoadBalancerSheddingIntervalMinutes());
        this.loadSheddingTask = loadManagerExecutor.scheduleAtFixedRate(
                this::execute, loadSheddingInterval, loadSheddingInterval, TimeUnit.MILLISECONDS);
    }

    @Override
    public void close() {
        if (this.loadSheddingTask != null) {
            this.loadSheddingTask.cancel(false);
        }
    }

    private boolean isLeader() {
        return pulsar.getLeaderElectionService() != null && pulsar.getLeaderElectionService().isLeader();
    }
}
