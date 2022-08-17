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
package org.apache.pulsar.broker.loadbalance.extensible.data;

import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import lombok.Getter;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.naming.TopicDomain;
import org.apache.pulsar.policies.data.loadbalancer.NamespaceBundleStats;

@Getter
public class TopBundlesLoadData {

    public static final String TOPIC =
            TopicDomain.persistent
                    + "://"
                    + NamespaceName.SYSTEM_NAMESPACE
                    + "/top-bundle-load-data";

    private final List<Pair<String, NamespaceBundleStats>> topBundlesLoadData;

    private TopBundlesLoadData(Map<String, NamespaceBundleStats> bundleStats, int topK) {
        topBundlesLoadData = bundleStats.entrySet()
                .stream()
                .map(entry -> Pair.of(entry.getKey(), entry.getValue()))
                .sorted(Comparator.naturalOrder())
                .limit(topK)
                .collect(Collectors.toList());
    }

    public static TopBundlesLoadData of(Map<String, NamespaceBundleStats> bundleStats, int topK) {
        return new TopBundlesLoadData(bundleStats, topK);
    }
}
