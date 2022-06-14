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

import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.policies.data.loadbalancer.NamespaceBundleStats;

import java.util.Map;

/**
 * Abstract load data reporter.
 *
 * @param <T> Load data type.
 */
public abstract class AbstractLoadDataReporter<T> implements LoadDataReporter<T> {

    protected double percentChange(final double oldValue, final double newValue) {
        if (oldValue == 0) {
            if (newValue == 0) {
                // Avoid NaN
                return 0;
            }
            return Double.POSITIVE_INFINITY;
        }
        return 100 * Math.abs((oldValue - newValue) / oldValue);
    }

    protected Map<String, NamespaceBundleStats> getBundleStats(PulsarService pulsar) {
        return pulsar.getBrokerService().getBundleStats();
    }

}
