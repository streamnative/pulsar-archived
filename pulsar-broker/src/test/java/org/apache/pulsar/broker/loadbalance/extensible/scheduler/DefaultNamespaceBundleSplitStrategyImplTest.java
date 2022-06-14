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

import static org.testng.Assert.assertTrue;
import static org.testng.AssertJUnit.assertFalse;

import java.util.Set;
import org.apache.pulsar.broker.loadbalance.extensible.BaseLoadManagerContext;
import org.apache.pulsar.broker.loadbalance.extensible.ExtensibleLoadManagerTestBase;
import org.apache.pulsar.broker.loadbalance.extensible.data.BrokerLoadData;
import org.apache.pulsar.broker.loadbalance.impl.LoadManagerShared;
import org.apache.pulsar.broker.namespace.NamespaceService;
import org.apache.pulsar.policies.data.loadbalancer.BundleData;
import org.apache.pulsar.policies.data.loadbalancer.NamespaceBundleStats;
import org.mockito.Mockito;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


/**
 * The unit test for {@link DefaultNamespaceBundleSplitStrategyImpl}.
 */
public class DefaultNamespaceBundleSplitStrategyImplTest extends ExtensibleLoadManagerTestBase {

    private BaseLoadManagerContext context;

    private NamespaceBundleSplitStrategy strategy;

    private NamespaceService namespaceService;

    @BeforeMethod
    public void setUp() {
        context = newBaseLoadManagerContext();
        strategy = new DefaultNamespaceBundleSplitStrategyImpl();
        namespaceService = Mockito.mock(NamespaceService.class);
    }

    @AfterMethod
    public void tearDown() {
        context = null;
        strategy = null;
        namespaceService = null;
    }

    @Test
    public void testEmptyBrokerLoadData() {
        Set<String> bundlesToSplit = strategy.findBundlesToSplit(context, namespaceService);
        assertTrue(bundlesToSplit.isEmpty());
    }

    @Test
    public void testEmptyBundleLoadDataAndMaxBundleTopic() throws Exception {
        BrokerLoadData brokerLoadData = new BrokerLoadData();
        NamespaceBundleStats namespaceBundleStats = new NamespaceBundleStats();
        namespaceBundleStats.topics = context.brokerConfiguration().getLoadBalancerNamespaceBundleMaxTopics() + 1;
        brokerLoadData.getLastStats().put("property/cluster/namespace/0x00000000_0xFFFFFFFF", namespaceBundleStats);
        context.brokerLoadDataStore().push("broker-1", brokerLoadData);
        Set<String> bundlesToSplit = strategy.findBundlesToSplit(context, namespaceService);
        assertFalse(bundlesToSplit.isEmpty());
    }

    @Test
    public void testMaxBundleTopicAndArriveMaxBundleCount() throws Exception {
        Mockito.when(namespaceService.getBundleCount(Mockito.any()))
                .thenReturn(context.brokerConfiguration().getLoadBalancerNamespaceMaximumBundles() + 1);
        BrokerLoadData brokerLoadData = new BrokerLoadData();
        NamespaceBundleStats namespaceBundleStats = new NamespaceBundleStats();
        namespaceBundleStats.topics = context.brokerConfiguration().getLoadBalancerNamespaceBundleMaxTopics() + 1;
        brokerLoadData.getLastStats().put("property/cluster/namespace/0x00000000_0xFFFFFFFF", namespaceBundleStats);
        context.brokerLoadDataStore().push("broker-1", brokerLoadData);
        Set<String> bundlesToSplit = strategy.findBundlesToSplit(context, namespaceService);
        assertTrue(bundlesToSplit.isEmpty());
    }

    @Test
    public void testTopicLessThanTwo() throws Exception {
        BrokerLoadData brokerLoadData = newBrokerLoadData();
        NamespaceBundleStats namespaceBundleStats = new NamespaceBundleStats();
        namespaceBundleStats.topics = 1;
        brokerLoadData.getLastStats().put("property/cluster/namespace/0x00000000_0xFFFFFFFF", namespaceBundleStats);
        context.brokerLoadDataStore().push("broker-1", brokerLoadData);
        Set<String> bundlesToSplit = strategy.findBundlesToSplit(context, namespaceService);
        assertTrue(bundlesToSplit.isEmpty());
    }

    @Test
    public void testMaxBundleSessions() throws Exception {
        BrokerLoadData brokerLoadData = newBrokerLoadData();
        NamespaceBundleStats namespaceBundleStats = new NamespaceBundleStats();
        namespaceBundleStats.topics = context.brokerConfiguration().getLoadBalancerNamespaceBundleMaxTopics() - 1;
        namespaceBundleStats.producerCount =
                context.brokerConfiguration().getLoadBalancerNamespaceBundleMaxSessions() / 2;
        namespaceBundleStats.consumerCount =
                (context.brokerConfiguration().getLoadBalancerNamespaceBundleMaxSessions() / 2) + 10;
        brokerLoadData.getLastStats().put("property/cluster/namespace/0x00000000_0xFFFFFFFF", namespaceBundleStats);
        context.brokerLoadDataStore().push("broker-1", brokerLoadData);
        Set<String> bundlesToSplit = strategy.findBundlesToSplit(context, namespaceService);
        assertFalse(bundlesToSplit.isEmpty());
    }

    @Test
    public void testMaxBundleMsgRate() throws Exception {
        BrokerLoadData brokerLoadData = newBrokerLoadData();
        NamespaceBundleStats namespaceBundleStats = new NamespaceBundleStats();
        namespaceBundleStats.topics = context.brokerConfiguration().getLoadBalancerNamespaceBundleMaxTopics() - 1;
        brokerLoadData.getLastStats().put("property/cluster/namespace/0x00000000_0xFFFFFFFF", namespaceBundleStats);
        context.brokerLoadDataStore().push("broker-1", brokerLoadData);

        BundleData bundleData = BundleData.newDefaultBundleData();
        namespaceBundleStats.msgRateIn = context.brokerConfiguration().getLoadBalancerNamespaceBundleMaxMsgRate() + 1;
        bundleData.update(namespaceBundleStats);
        context.bundleLoadDataStore().push("property/cluster/namespace/0x00000000_0xFFFFFFFF", bundleData);
        Set<String> bundlesToSplit = strategy.findBundlesToSplit(context, namespaceService);
        assertFalse(bundlesToSplit.isEmpty());
    }

    @Test
    public void testMaxBundleBandwidth() throws Exception {
        BrokerLoadData brokerLoadData = newBrokerLoadData();
        NamespaceBundleStats namespaceBundleStats = new NamespaceBundleStats();
        namespaceBundleStats.topics = context.brokerConfiguration().getLoadBalancerNamespaceBundleMaxTopics() - 1;
        brokerLoadData.getLastStats().put("property/cluster/namespace/0x00000000_0xFFFFFFFF", namespaceBundleStats);
        context.brokerLoadDataStore().push("broker-1", brokerLoadData);

        BundleData bundleData = BundleData.newDefaultBundleData();
        namespaceBundleStats.msgThroughputIn = context
                .brokerConfiguration()
                .getLoadBalancerNamespaceBundleMaxBandwidthMbytes() * LoadManagerShared.MIBI + 1;
        bundleData.update(namespaceBundleStats);
        context.bundleLoadDataStore().push("property/cluster/namespace/0x00000000_0xFFFFFFFF", bundleData);
        Set<String> bundlesToSplit = strategy.findBundlesToSplit(context, namespaceService);
        assertFalse(bundlesToSplit.isEmpty());
    }
}