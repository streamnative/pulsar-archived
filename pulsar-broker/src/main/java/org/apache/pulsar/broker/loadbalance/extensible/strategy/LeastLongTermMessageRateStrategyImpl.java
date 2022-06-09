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
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ThreadLocalRandom;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.loadbalance.extensible.BaseLoadManagerContext;
import org.apache.pulsar.broker.loadbalance.extensible.data.BrokerLoadData;
import org.apache.pulsar.policies.data.loadbalancer.BundleData;
import org.apache.pulsar.policies.data.loadbalancer.TimeAverageBrokerData;
import org.apache.pulsar.policies.data.loadbalancer.TimeAverageMessageData;

/**
 * Placement strategy which selects a broker based on which one has the least long term message rate.
 * See {@link org.apache.pulsar.broker.loadbalance.impl.LeastLongTermMessageRate}
 */
@Slf4j
public class LeastLongTermMessageRateStrategyImpl extends AbstractBrokerSelectionStrategy {

    private static final String LOAD_BALANCER_NAME = "LeastLongTermMessageRateStrategy";

    private final ArrayList<String> bestBrokers;

    public LeastLongTermMessageRateStrategyImpl() {
        bestBrokers = new ArrayList<>();
    }

    @Override
    public Optional<String> doSelect(List<String> brokers, BaseLoadManagerContext context) {
        double minScore = Double.POSITIVE_INFINITY;
        // Maintain of list of all the best scoring brokers and then randomly
        // select one of them at the end.
        for (String broker : brokers) {
            final double score = getScore(context, broker, context.brokerConfiguration());
            if (score == Double.POSITIVE_INFINITY) {
                final Optional<BrokerLoadData> localDataOpt = context.brokerLoadDataStore().get(broker);
                if (localDataOpt.isPresent()) {
                    BrokerLoadData localData = localDataOpt.get();
                    log.warn("Broker {} is overloaded: CPU: {}%, MEMORY: {}%, DIRECT MEMORY: {}%, BANDWIDTH IN: {}%, "
                                    + "BANDWIDTH OUT: {}%",
                            broker, localData.getCpu().percentUsage(), localData.getMemory().percentUsage(),
                            localData.getDirectMemory().percentUsage(), localData.getBandwidthIn().percentUsage(),
                            localData.getBandwidthOut().percentUsage());
                }
            }
            if (score < minScore) {
                // Clear best brokers since this score beats the other brokers.
                bestBrokers.clear();
                bestBrokers.add(broker);
                minScore = score;
            } else if (score == minScore) {
                // Add this broker to best brokers since it ties with the best score.
                bestBrokers.add(broker);
            }
        }
        if (bestBrokers.isEmpty()) {
            // All brokers are overloaded.
            // Assign randomly in this case.
            bestBrokers.addAll(brokers);
        }

        if (bestBrokers.isEmpty()) {
            // If still, it means there are no available brokers at this point
            return Optional.empty();
        }

        return Optional.of(bestBrokers.get(ThreadLocalRandom.current().nextInt(bestBrokers.size())));
    }

    // Form a score for a broker using its preallocated bundle data and time average data.
    // This is done by summing all preallocated long-term message rates and adding them to the broker's overall
    // long-term message rate, which is itself the sum of the long-term message rate of every allocated bundle.
    // Any broker at (or above) the overload threshold will have a score of POSITIVE_INFINITY.
    private static double getScore(final BaseLoadManagerContext context,
                                   final String broker,
                                   final ServiceConfiguration conf) {
        final double overloadThreshold = conf.getLoadBalancerBrokerOverloadedThresholdPercentage() / 100.0;
        Optional<BrokerLoadData> brokerLoadDataOpt = context.brokerLoadDataStore().get(broker);
        final double maxUsage = brokerLoadDataOpt.map(BrokerLoadData::getMaxResourceUsage).orElse(0.0);

        if (maxUsage > overloadThreshold) {
            log.warn("Broker {} is overloaded: max usage={}", broker, maxUsage);
            return Double.POSITIVE_INFINITY;
        }

        double totalMessageRate = 0;
        for (BundleData bundleData : context.preallocatedBundleData(broker).values()) {
            final TimeAverageMessageData longTermData = bundleData.getLongTermData();
            totalMessageRate += longTermData.getMsgRateIn() + longTermData.getMsgRateOut();
        }

        // calculate estimated score
        final Optional<TimeAverageBrokerData> timeAverageDataOpt = context.timeAverageBrokerLoadDataStore().get(broker);
        final double timeAverageLongTermMessageRate = timeAverageDataOpt.map(timeAverageBrokerData ->
                        timeAverageBrokerData.getLongTermMsgRateIn() + timeAverageBrokerData.getLongTermMsgRateOut())
                .orElse(0.0);

        final double totalMessageRateEstimate = totalMessageRate + timeAverageLongTermMessageRate;

        if (log.isDebugEnabled()) {
            log.debug("Broker {} has long term message rate {}", broker, totalMessageRateEstimate);
        }
        return totalMessageRateEstimate;
    }

    @Override
    public String name() {
        return LOAD_BALANCER_NAME;
    }
}
