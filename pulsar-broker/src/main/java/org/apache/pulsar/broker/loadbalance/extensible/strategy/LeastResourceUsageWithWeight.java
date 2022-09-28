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
package org.apache.pulsar.broker.loadbalance.extensible.strategy;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ThreadLocalRandom;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.loadbalance.extensible.BaseLoadManagerContext;
import org.apache.pulsar.broker.loadbalance.extensible.data.BrokerLoadData;
import org.apache.pulsar.common.naming.ServiceUnitId;

@Slf4j
public class LeastResourceUsageWithWeight extends AbstractBrokerSelectionStrategy {

    // Maintain this list to reduce object creation.
    private final ArrayList<String> bestBrokers;
    private final Map<String, Double> brokerAvgResourceUsageWithWeight;

    public LeastResourceUsageWithWeight() {
        this.bestBrokers = new ArrayList<>();
        this.brokerAvgResourceUsageWithWeight = new HashMap<>();
    }

    // A broker's max resource usage with weight using its historical load and short-term load data with weight.
    private double getMaxResourceUsageWithWeight(final String broker, final BrokerLoadData brokerData,
                                                 final ServiceConfiguration conf) {
        final double overloadThreshold = conf.getLoadBalancerBrokerOverloadedThresholdPercentage() / 100.0;
        final double maxUsageWithWeight =
                updateAndGetMaxResourceUsageWithWeight(broker, brokerData, conf);

        if (maxUsageWithWeight > overloadThreshold) {
            log.warn(
                    "Broker {} is overloaded, max resource usage with weight percentage: {}%, "
                            + "CPU: {}%, MEMORY: {}%, DIRECT MEMORY: {}%, BANDWIDTH IN: {}%, "
                            + "BANDWIDTH OUT: {}%, CPU weight: {}, MEMORY weight: {}, DIRECT MEMORY weight: {}, "
                            + "BANDWIDTH IN weight: {}, BANDWIDTH OUT weight: {}",
                    broker, maxUsageWithWeight * 100,
                    brokerData.getCpu().percentUsage(), brokerData.getMemory().percentUsage(),
                    brokerData.getDirectMemory().percentUsage(), brokerData.getBandwidthIn().percentUsage(),
                    brokerData.getBandwidthOut().percentUsage(), conf.getLoadBalancerCPUResourceWeight(),
                    conf.getLoadBalancerMemoryResourceWeight(), conf.getLoadBalancerDirectMemoryResourceWeight(),
                    conf.getLoadBalancerBandwithInResourceWeight(),
                    conf.getLoadBalancerBandwithOutResourceWeight());
        }

        if (log.isDebugEnabled()) {
            log.debug("Broker {} has max resource usage with weight percentage: {}%",
                    broker, maxUsageWithWeight * 100);
        }
        return maxUsageWithWeight;
    }

    /**
     * Update and get the max resource usage with weight of broker according to the service configuration.
     *
     * @param broker     the broker name.
     * @param brokerData The broker load data.
     * @param conf       The service configuration.
     * @return the max resource usage with weight of broker
     */
    private double updateAndGetMaxResourceUsageWithWeight(String broker, BrokerLoadData brokerData,
                                                          ServiceConfiguration conf) {
        final double historyPercentage = conf.getLoadBalancerHistoryResourcePercentage();
        Double historyUsage = brokerAvgResourceUsageWithWeight.get(broker);
        double resourceUsage = brokerData.getMaxResourceUsageWithinLimit(conf);
        historyUsage = historyUsage == null
                ? resourceUsage : historyUsage * historyPercentage + (1 - historyPercentage) * resourceUsage;
        if (log.isDebugEnabled()) {
            log.debug(
                    "Broker {} get max resource usage with weight: {}, history resource percentage: {}%, CPU weight: "
                            + "{}, MEMORY weight: {}, DIRECT MEMORY weight: {}, BANDWIDTH IN weight: {}, BANDWIDTH "
                            + "OUT weight: {} ",
                    broker, historyUsage, historyPercentage, conf.getLoadBalancerCPUResourceWeight(),
                    conf.getLoadBalancerMemoryResourceWeight(), conf.getLoadBalancerDirectMemoryResourceWeight(),
                    conf.getLoadBalancerBandwithInResourceWeight(),
                    conf.getLoadBalancerBandwithOutResourceWeight());
        }
        brokerAvgResourceUsageWithWeight.put(broker, historyUsage);
        return historyUsage;
    }

    @Override
    public Optional<String> doSelect(List<String> candidates, ServiceUnitId bundleId, BaseLoadManagerContext context) {
        String bundle = bundleId.toString();
        if (candidates.isEmpty()) {
            log.info("There are no available brokers as candidates at this point for bundle: {}", bundle);
            return Optional.empty();
        }

        val conf = context.brokerConfiguration();
        bestBrokers.clear();
        // Maintain of list of all the best scoring brokers and then randomly
        // select one of them at the end.
        double totalUsage = 0.0d;
        for (String broker : candidates) {
            Optional<BrokerLoadData> brokerData = context.brokerLoadDataStore().get(broker);
            if (brokerData.isEmpty()) {
                log.error("there is no broker load data for broker: {}. Skipping this select", broker);

                // TODO: is random best choice?
                return selectRandomBroker(candidates);
            }

            double usageWithWeight = getMaxResourceUsageWithWeight(broker, brokerData.get(), conf);
            totalUsage += usageWithWeight;

        }

        final double avgUsage = totalUsage / candidates.size();
        final double diffThreshold =
                conf.getLoadBalancerAverageResourceUsageDifferenceThresholdPercentage() / 100.0;
        candidates.forEach(broker -> {
            Double avgResUsage = brokerAvgResourceUsageWithWeight.getOrDefault(broker, Double.MAX_VALUE);
            if ((avgResUsage + diffThreshold <= avgUsage)) {
                bestBrokers.add(broker);
            }
        });

        if (bestBrokers.isEmpty()) {
            // Assign randomly as all brokers are overloaded.
            log.warn("Assign randomly as all {} brokers are overloaded.", candidates.size());
            bestBrokers.addAll(candidates);
        }

        if (log.isDebugEnabled()) {
            log.debug("Selected {} best brokers: {} from candidate brokers: {}", bestBrokers.size(), bestBrokers,
                    candidates);
        }
        return selectRandomBroker(bestBrokers);
    }

    private static Optional<String> selectRandomBroker(List<String> candidates) {
        return Optional.of(candidates.get(ThreadLocalRandom.current().nextInt(candidates.size() - 1)));
    }

    @Override
    public String name() {
        return LeastResourceUsageWithWeight.class.getSimpleName();
    }
}
