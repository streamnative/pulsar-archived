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

import static org.testng.Assert.assertEquals;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.loadbalance.BrokerFilterException;
import org.apache.pulsar.broker.loadbalance.extensible.BaseLoadManagerContext;
import org.apache.pulsar.broker.loadbalance.extensible.ExtensibleLoadManagerTestBase;
import org.apache.pulsar.policies.data.loadbalancer.BundleData;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * The unit test for {@link LargeTopicCountFilter}.
 */
public class LargeTopicCountFilterTest extends ExtensibleLoadManagerTestBase {

    private LargeTopicCountFilter filter;

    private BaseLoadManagerContext context;

    @BeforeMethod
    public void setUp() {
        filter = new LargeTopicCountFilter();
        context = newBaseLoadManagerContext();
    }

    @AfterMethod
    public void tearDown() {
        filter = null;
        context = null;
    }

    @Test
    public void testOneBrokerOneBundleTopicSizeExceedingTheLimit() throws BrokerFilterException {
        ServiceConfiguration conf = context.brokerConfiguration();
        List<String> brokers = new ArrayList<>(Arrays.asList("broker-1", "broker-2"));

        // Make broker-1 topic size exceeding the limit.
        Map<String, BundleData> preallocatedBundleData = context.preallocatedBundleData("broker-1");
        BundleData bundleData = new BundleData();
        bundleData.setTopics(conf.getLoadBalancerBrokerMaxTopics() + 10);
        preallocatedBundleData.put("bundle-1", bundleData);

        filter.filter(brokers, context);

        assertEquals(brokers.size(), 1);
        assertEquals(brokers.get(0), "broker-2");
    }

    @Test
    public void testOneBrokerMultipleBundleTopicSizeSumExceedingTheLimit() throws BrokerFilterException {
        ServiceConfiguration conf = context.brokerConfiguration();
        int bundleCount = 100;
        conf.setLoadBalancerBrokerMaxTopics(bundleCount * 2);

        List<String> brokers = new ArrayList<>(Arrays.asList("broker-1", "broker-2"));

        Map<String, BundleData> preallocatedBundleData = context.preallocatedBundleData("broker-1");
        int loadBalancerBrokerMaxTopics = conf.getLoadBalancerBrokerMaxTopics();
        for(int i = 0; i < bundleCount; i++) {
            String bundle = "bundle-" + i;
            BundleData bundleData = new BundleData();
            int topics = ThreadLocalRandom.current().nextInt(0, loadBalancerBrokerMaxTopics);
            loadBalancerBrokerMaxTopics -= topics;
            bundleData.setTopics(topics + 1);
            preallocatedBundleData.put(bundle, bundleData);
        }

        filter.filter(brokers, context);

        assertEquals(brokers.size(), 1);
        assertEquals(brokers.get(0), "broker-2");
    }
}