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
package org.apache.pulsar.broker.loadbalance.extensible;

import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertNotNull;

import com.google.common.hash.Hashing;
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.util.ZkUtils;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.loadbalance.LoadBalancerTestingUtils;
import org.apache.pulsar.broker.loadbalance.extensible.data.LoadDataStore;
import org.apache.pulsar.broker.loadbalance.extensible.data.LoadDataStoreFactory;
import org.apache.pulsar.common.naming.NamespaceBundle;
import org.apache.pulsar.common.naming.NamespaceBundleFactory;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.common.util.ObjectMapperFactory;
import org.apache.pulsar.policies.data.loadbalancer.BundleData;
import org.apache.pulsar.policies.data.loadbalancer.TimeAverageMessageData;
import org.apache.pulsar.zookeeper.LocalBookkeeperEnsemble;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.awaitility.Awaitility;
import org.powermock.reflect.Whitebox;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;


@Slf4j
public class ExtensibleLoadManagerTest {

    private ExecutorService executor;

    private LocalBookkeeperEnsemble bkEnsemble;

    private PulsarService pulsar1;

    private PulsarService pulsar2;

    private NamespaceBundleFactory nsFactory;

    private BrokerDiscoveryImpl primaryBrokerDiscovery;

    private BrokerDiscoveryImpl secondaryBrokerDiscovery;

    @BeforeMethod
    void setup() throws Exception {
        executor = new ThreadPoolExecutor(5, 20, 30, TimeUnit.SECONDS,
                new LinkedBlockingQueue<>());
        // Start local bookkeeper ensemble
        bkEnsemble = new LocalBookkeeperEnsemble(3, 0, () -> 0);
        bkEnsemble.start();

        // Start broker 1
        ServiceConfiguration config1 = new ServiceConfiguration();
        config1.setLoadBalancerEnabled(true);
        config1.setLoadManagerClassName(BrokerDiscoveryImpl.class.getName());
        config1.setClusterName("use");
        config1.setWebServicePort(Optional.of(0));
        config1.setMetadataStoreUrl("zk:127.0.0.1" + ":" + bkEnsemble.getZookeeperPort());
        config1.setBrokerShutdownTimeoutMs(0L);
        config1.setBrokerServicePort(Optional.of(0));
        config1.setAdvertisedAddress("localhost");
        createCluster(bkEnsemble.getZkClient(), config1);
        pulsar1 = new PulsarService(config1);
        pulsar1.start();

        // Start broker 2
        ServiceConfiguration config2 = new ServiceConfiguration();
        config2.setLoadBalancerEnabled(true);
        config2.setLoadManagerClassName(BrokerDiscoveryImpl.class.getName());
        config2.setClusterName("use");
        config2.setWebServicePort(Optional.of(0));
        config2.setMetadataStoreUrl("zk:127.0.0.1" + ":" + bkEnsemble.getZookeeperPort());
        config2.setBrokerShutdownTimeoutMs(0L);
        config2.setBrokerServicePort(Optional.of(0));
        config2.setAdvertisedAddress("localhost");
        pulsar2 = new PulsarService(config2);
        pulsar2.start();

        nsFactory = new NamespaceBundleFactory(pulsar1, Hashing.crc32());

        primaryBrokerDiscovery = Whitebox.getInternalState(pulsar1.getLoadManager().get(), "brokerDiscovery");
        secondaryBrokerDiscovery = Whitebox.getInternalState(pulsar2.getLoadManager().get(), "brokerDiscovery");
    }

    @AfterMethod(alwaysRun = true)
    void shutdown() throws Exception {
        log.info("--- Shutting down ---");
        executor.shutdownNow();

        pulsar2.close();
        pulsar1.close();

        bkEnsemble.stop();
    }

    /**
     * It verifies that once broker owns max-number of topics: load-manager doesn't allocates new bundles to that broker
     * unless all the brokers are in same state.
     *
     * <pre>
     * 1. Create a bundle whose bundle-resource-quota will contain max-topics
     * 2. Load-manager assigns broker to this bundle so, assigned broker is overloaded with max-topics
     * 3. For any new further bundles: broker assigns different brokers.
     * </pre>
     *
     */
    @Test
    public void testMaxTopicDistributionToBroker() throws Exception {

        final int totalBundles = 50;
        final NamespaceBundle[] bundles = LoadBalancerTestingUtils
                .makeBundles(nsFactory, "test", "test", "test", totalBundles);

        final BundleData bundleData = new BundleData(10, 1000);
        // it sets max topics under this bundle so, owner of this broker reaches max-topic threshold
        bundleData.setTopics(pulsar1.getConfiguration().getLoadBalancerBrokerMaxTopics() + 10);
        final TimeAverageMessageData longTermMessageData = new TimeAverageMessageData(1000);
        longTermMessageData.setMsgRateIn(1000);
        bundleData.setLongTermData(longTermMessageData);

        @Cleanup
        LoadDataStore<BundleData> bundleLoadDataStore =
                LoadDataStoreFactory.create(pulsar1, BrokerDiscoveryImpl.BUNDLE_LOAD_DATA_STORE_NAME, BundleData.class);
        bundleLoadDataStore.push(bundles[0].toString(), bundleData);
        Awaitility.await().untilAsserted(() -> assertNotNull(bundleLoadDataStore.get(bundles[0].toString())));

        String maxTopicOwnedBroker = primaryBrokerDiscovery.discover(bundles[0]).get();

        for (int i = 1; i < totalBundles; i++) {
            assertNotEquals(primaryBrokerDiscovery.discover(bundles[i]).get(), maxTopicOwnedBroker);
        }

    }

    private void createCluster(ZooKeeper zk, ServiceConfiguration config) throws Exception {
        ZkUtils.createFullPathOptimistic(zk, "/admin/clusters/" + config.getClusterName(),
                ObjectMapperFactory.getThreadLocal().writeValueAsBytes(
                        ClusterData.builder()
                                .serviceUrl("http://" + config.getAdvertisedAddress() + ":" + config.getWebServicePort()
                                        .get())
                                .build()),
                ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
    }
}
