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
package org.apache.pulsar.broker.loadbalance.extensible.filter;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import org.apache.pulsar.broker.loadbalance.extensible.BaseLoadManagerContext;
import org.apache.pulsar.broker.loadbalance.extensible.data.BrokerLoadData;
import org.apache.pulsar.policies.data.loadbalancer.BundleData;

/**
 * Large topic count filter.
 */
public class LargeTopicCountFilter extends BaseBrokerFilter {

    public static final String FILTER_NAME = "large_topic_count_filter";

    @Override
    void doFilter(List<String> brokers, BaseLoadManagerContext context) {
        int loadBalancerBrokerMaxTopics = context.brokerConfiguration().getLoadBalancerBrokerMaxTopics();
        List<String> filteredBrokerCandidates = brokers.stream().filter((broker) -> {
            Optional<BrokerLoadData> brokerLoadDataOpt = context.brokerLoadDataStore().get(broker);
            long totalTopics = context.preallocatedBundleData(broker)
                    .values()
                    .stream()
                    .mapToLong(BundleData::getTopics).sum()
                    + (brokerLoadDataOpt.map(BrokerLoadData::getNumTopics).orElse(0));
            return totalTopics <= loadBalancerBrokerMaxTopics;
        }).collect(Collectors.toList());

        if (!filteredBrokerCandidates.isEmpty()) {
            brokers.clear();
            brokers.addAll(filteredBrokerCandidates);
        }
    }

    @Override
    public String name() {
        return FILTER_NAME;
    }
}
