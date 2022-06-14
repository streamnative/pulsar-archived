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

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import java.util.HashMap;
import java.util.Map;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;
import com.google.common.collect.Sets;
import org.apache.pulsar.broker.loadbalance.extensible.BaseLoadManagerContext;
import org.apache.pulsar.broker.loadbalance.extensible.ExtensibleLoadManagerTestBase;
import org.apache.pulsar.broker.loadbalance.extensible.data.BrokerLoadData;
import org.apache.pulsar.policies.data.loadbalancer.BundleData;
import org.apache.pulsar.policies.data.loadbalancer.ResourceUsage;
import org.apache.pulsar.policies.data.loadbalancer.TimeAverageMessageData;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * The unit test for {@link OverloadShedderUnloadStrategy}.
 */
public class OverloadShedderUnloadStrategyTest extends ExtensibleLoadManagerTestBase {

    private OverloadShedderUnloadStrategy strategy;

    private BaseLoadManagerContext context;

    private Map<String, Long> recentlyUnloadedBundles;


    @BeforeMethod
    public void setUp() {
        strategy = new OverloadShedderUnloadStrategy();
        context = newBaseLoadManagerContext();
        context.brokerConfiguration().setLoadBalancerBrokerOverloadedThresholdPercentage(85);
        recentlyUnloadedBundles = new HashMap<>();
    }

    @AfterMethod
    public void tearDown() {
        strategy = null;
        context = null;
        recentlyUnloadedBundles = null;
    }

    @Test
    public void testNoBrokers() {
        Multimap<String, String> bundlesForUnloading =
                strategy.findBundlesForUnloading(context, recentlyUnloadedBundles);
        assertTrue(bundlesForUnloading.isEmpty());
    }

    @Test
    public void testBrokersWithNoBundles() throws Exception {
        BrokerLoadData brokerLoadData = newBrokerLoadData();
        brokerLoadData.setBandwidthIn(new ResourceUsage(999, 1000));
        brokerLoadData.setBandwidthOut(new ResourceUsage(999, 1000));
        context.brokerLoadDataStore().push("broker-1", brokerLoadData);
        Multimap<String, String> bundlesForUnloading =
                strategy.findBundlesForUnloading(context, recentlyUnloadedBundles);
        assertTrue(bundlesForUnloading.isEmpty());
    }

    @Test
    public void testBrokerNotOverloaded() throws Exception {
        BrokerLoadData brokerLoadData = newBrokerLoadData();
        brokerLoadData.setBandwidthIn(new ResourceUsage(500, 1000));
        brokerLoadData.setBandwidthOut(new ResourceUsage(500, 1000));
        brokerLoadData.setBundles(Sets.newHashSet("bundle-1"));

        BundleData bundle1 = new BundleData();
        TimeAverageMessageData db1 = new TimeAverageMessageData();
        db1.setMsgThroughputIn(1000);
        db1.setMsgThroughputOut(1000);
        bundle1.setShortTermData(db1);

        context.bundleLoadDataStore().push("bundle-1", bundle1);
        context.brokerLoadDataStore().push("broker-1", brokerLoadData);

        Multimap<String, String> bundlesForUnloading =
                strategy.findBundlesForUnloading(context, recentlyUnloadedBundles);
        assertTrue(bundlesForUnloading.isEmpty());
    }

    @Test
    public void testBrokerWithSingleBundle() throws Exception {
        BrokerLoadData brokerLoadData = newBrokerLoadData();
        brokerLoadData.setBandwidthIn(new ResourceUsage(999, 1000));
        brokerLoadData.setBandwidthOut(new ResourceUsage(999, 1000));
        brokerLoadData.setBundles(Sets.newHashSet("bundle-1"));

        BundleData bundle1 = new BundleData();
        TimeAverageMessageData db1 = new TimeAverageMessageData();
        db1.setMsgThroughputIn(1000);
        db1.setMsgThroughputOut(1000);
        bundle1.setShortTermData(db1);

        context.bundleLoadDataStore().push("bundle-1", bundle1);
        context.brokerLoadDataStore().push("broker-1", brokerLoadData);

        Multimap<String, String> bundlesForUnloading =
                strategy.findBundlesForUnloading(context, recentlyUnloadedBundles);
        assertTrue(bundlesForUnloading.isEmpty());
    }

    @Test
    public void testBrokerWithMultipleBundles() throws Exception {
        int numBundles = 10;

        BrokerLoadData broker1 = newBrokerLoadData();
        broker1.setBandwidthIn(new ResourceUsage(999, 1000));
        broker1.setBandwidthOut(new ResourceUsage(999, 1000));
        broker1.setBundles(Sets.newHashSet("bundle-1"));

        BrokerLoadData anotherBroker = newBrokerLoadData();
        String anotherBrokerName = "another-broker";

        double brokerThroghput = 0;

        for (int i = 1; i <= numBundles; i++) {
            broker1.getBundles().add("bundle-" + i);

            BundleData bundle = new BundleData();
            TimeAverageMessageData db = new TimeAverageMessageData();

            double throughput = i * 1024 * 1024;
            db.setMsgThroughputIn(throughput);
            db.setMsgThroughputOut(throughput);
            bundle.setShortTermData(db);
            context.bundleLoadDataStore().push("bundle-" + i, bundle);

            // This bundle should not be selected for `broker1` since it is belong to another broker.
            String anotherBundleName = anotherBrokerName + "-bundle-" + (numBundles + i);
            context.bundleLoadDataStore().push(anotherBundleName, bundle);
            anotherBroker.getBundles().add(anotherBundleName);
            brokerThroghput += throughput;
        }

        broker1.setMsgThroughputIn(brokerThroghput);
        broker1.setMsgThroughputOut(brokerThroghput);


        context.brokerLoadDataStore().push("broker-1", broker1);
        context.brokerLoadDataStore().push(anotherBrokerName, anotherBroker);

        Multimap<String, String> bundlesToUnload = strategy.findBundlesForUnloading(context, recentlyUnloadedBundles);
        assertFalse(bundlesToUnload.isEmpty());
        assertEquals(bundlesToUnload.get("broker-1"), Lists.newArrayList("bundle-10", "bundle-9"));
    }

    @Test
    public void testFilterRecentlyUnloaded() throws Exception {
        int numBundles = 10;

        BrokerLoadData broker1 = newBrokerLoadData();
        broker1.setBandwidthIn(new ResourceUsage(999, 1000));
        broker1.setBandwidthOut(new ResourceUsage(999, 1000));

        double brokerThroghput = 0;

        for (int i = 1; i <= numBundles; i++) {
            broker1.getBundles().add("bundle-" + i);

            BundleData bundle = new BundleData();
            TimeAverageMessageData db = new TimeAverageMessageData();

            double throughput = i * 1024 * 1024;
            db.setMsgThroughputIn(throughput);
            db.setMsgThroughputOut(throughput);
            bundle.setShortTermData(db);
            context.bundleLoadDataStore().push("bundle-" + i, bundle);

            brokerThroghput += throughput;
        }

        broker1.setMsgThroughputIn(brokerThroghput);
        broker1.setMsgThroughputOut(brokerThroghput);

        context.brokerLoadDataStore().push("broker-1", broker1);

        recentlyUnloadedBundles.put("bundle-10", 1L);
        recentlyUnloadedBundles.put("bundle-9", 1L);

        Multimap<String, String> bundlesToUnload = strategy.findBundlesForUnloading(context, recentlyUnloadedBundles);
        assertFalse(bundlesToUnload.isEmpty());
        assertEquals(bundlesToUnload.get("broker-1"), Lists.newArrayList("bundle-8", "bundle-7"));
    }

    @Test
    public void testPrintResourceUsage() {
        BrokerLoadData data = new BrokerLoadData();

        data.setCpu(new ResourceUsage(10, 100));
        data.setMemory(new ResourceUsage(50, 100));
        data.setDirectMemory(new ResourceUsage(90, 100));
        data.setBandwidthIn(new ResourceUsage(30, 100));
        data.setBandwidthOut(new ResourceUsage(20, 100));

        assertEquals(data.printResourceUsage(),
                "cpu: 10.00%, memory: 50.00%, directMemory: 90.00%, bandwidthIn: 30.00%, bandwidthOut: 20.00%");
    }
}