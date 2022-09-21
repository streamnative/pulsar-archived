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


import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

import java.net.URL;
import java.util.Collections;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import com.google.common.hash.Hashing;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.util.ZkUtils;
import org.apache.commons.lang3.reflect.FieldUtils;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.loadbalance.LeaderElectionService;
import org.apache.pulsar.broker.loadbalance.extensible.channel.BundleStateChannel;
import org.apache.pulsar.broker.loadbalance.extensible.channel.BundleStateData;
import org.apache.pulsar.broker.loadbalance.extensible.data.Split;
import org.apache.pulsar.broker.loadbalance.extensible.data.Unload;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.impl.TableViewImpl;
import org.apache.pulsar.common.naming.NamespaceBundleFactory;
import org.apache.pulsar.common.policies.data.BundlesData;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.common.policies.data.Policies;
import org.apache.pulsar.common.policies.data.TenantInfo;
import org.apache.pulsar.common.util.ObjectMapperFactory;
import org.apache.pulsar.zookeeper.LocalBookkeeperEnsemble;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.awaitility.Awaitility;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


@Slf4j
public class ExtensibleLoadManagerTest {

    private static final String CLUSTER_NAME = "use";

    private ExecutorService executor;

    private LocalBookkeeperEnsemble bkEnsemble;

    private PulsarService pulsar1;

    private PulsarService pulsar2;

    private NamespaceBundleFactory nsFactory;

    private ExtensibleLoadManagerImpl primaryLoadManager;

    private ExtensibleLoadManagerImpl secondaryLoadManager;

    private URL url1;

    private PulsarAdmin admin1;

    private URL url2;

    private PulsarAdmin admin2;

    private String primaryHost;

    private String secondaryHost;

    String namespace;

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
        config1.setLoadManagerClassName(ExtensibleLoadManagerImpl.class.getName());
        config1.setClusterName(CLUSTER_NAME);
        config1.setWebServicePort(Optional.of(0));
        config1.setMetadataStoreUrl("zk:127.0.0.1" + ":" + bkEnsemble.getZookeeperPort());
        config1.setBrokerShutdownTimeoutMs(0L);
        config1.setBrokerServicePort(Optional.of(0));
        config1.setAdvertisedAddress("localhost");
        createCluster(bkEnsemble.getZkClient(), config1);
        pulsar1 = new PulsarService(config1);
        pulsar1.start();

        primaryHost = String.format("%s:%d", "localhost", pulsar1.getListenPortHTTP().get());
        url1 = new URL(pulsar1.getWebServiceAddress());
        admin1 = PulsarAdmin.builder().serviceHttpUrl(url1.toString()).build();

        // Start broker 2
        ServiceConfiguration config2 = new ServiceConfiguration();
        config2.setLoadBalancerEnabled(true);
        config2.setLoadManagerClassName(ExtensibleLoadManagerImpl.class.getName());
        config2.setClusterName(CLUSTER_NAME);
        config2.setWebServicePort(Optional.of(0));
        config2.setMetadataStoreUrl("zk:127.0.0.1" + ":" + bkEnsemble.getZookeeperPort());
        config2.setBrokerShutdownTimeoutMs(0L);
        config2.setBrokerServicePort(Optional.of(0));
        config2.setAdvertisedAddress("localhost");
        pulsar2 = new PulsarService(config2);
        pulsar2.start();

        secondaryHost = String.format("%s:%d", "localhost", pulsar2.getListenPortHTTP().get());
        url2 = new URL(pulsar2.getWebServiceAddress());
        admin2 = PulsarAdmin.builder().serviceHttpUrl(url2.toString()).build();

        admin1.tenants().createTenant("public",
                TenantInfo.builder().allowedClusters(Collections.singleton(CLUSTER_NAME)).build());
        Policies policies = new Policies();
        policies.bundles = BundlesData.builder().numBundles(9).build();
        policies.replication_clusters = Collections.singleton(CLUSTER_NAME);
        namespace = "public/default";
        admin1.namespaces().createNamespace(namespace, policies);

        nsFactory = new NamespaceBundleFactory(pulsar1, Hashing.crc32());

        primaryLoadManager = (ExtensibleLoadManagerImpl)
                FieldUtils.readField(pulsar1.getLoadManager().get(), "loadManager", true);
        secondaryLoadManager = (ExtensibleLoadManagerImpl)
                FieldUtils.readField(pulsar2.getLoadManager().get(), "loadManager", true);
    }

    @AfterMethod(alwaysRun = true)
    void shutdown() throws Exception {
        log.info("--- Shutting down ---");
        executor.shutdownNow();

        pulsar2.close();
        pulsar1.close();

        bkEnsemble.stop();
    }

    @Test
    public void test() throws Exception {
        String topic = "test";
        admin1.topics().createPartitionedTopic(topic, 1);
        String lookupBroker = admin1.lookups().lookupTopic(topic);
        log.info("Topic {} broker url: {}", topic, lookupBroker);
        assertEquals(lookupBroker, admin2.lookups().lookupTopic(topic));
        String broker1LookupServiceAddress = primaryLoadManager.getBrokerRegistry().getLookupServiceAddress();
        String broker2LookupServiceAddress = secondaryLoadManager.getBrokerRegistry().getLookupServiceAddress();


        // bundle state channel tests
        // basic transfer test
        String bundle = String.format("%s/%s", namespace, admin1.lookups().getBundleRange(topic));
        log.info("bundle: {}", bundle);
        BundleStateChannel channel = primaryLoadManager.getBundleStateChannel();

        channel.publishAssignment(bundle, broker1LookupServiceAddress);
        channel.publishAssignment(bundle, broker2LookupServiceAddress);

        log.info("Conflict assignments, Topic {} broker url: {}", topic, admin2.lookups().lookupTopic(topic));


        String dstBroker = broker1LookupServiceAddress;
        String dstBrokerLookupAddress = pulsar1.getBrokerServiceUrl();
        PulsarService dstPulsar = pulsar1;
        PulsarAdmin dstAdmin = admin1;
        String srcBroker = broker2LookupServiceAddress;
        String srcBrokerLookupAddress = pulsar2.getBrokerServiceUrl();
        PulsarService srcPulsar = pulsar2;
        PulsarAdmin srcAdmin = admin2;

        if(pulsar1.getBrokerServiceUrl().equals(lookupBroker)){
            dstBroker = broker2LookupServiceAddress;
            dstBrokerLookupAddress = pulsar2.getBrokerServiceUrl();
            dstPulsar = pulsar2;
            dstAdmin = admin2;
            srcBroker = broker1LookupServiceAddress;
            srcBrokerLookupAddress = pulsar1.getBrokerServiceUrl();
            srcPulsar = pulsar1;
            srcAdmin = admin1;
        }
        Unload unload = new Unload(srcBroker, bundle, Optional.of(dstBroker));
        channel.publishUnload(unload);
        waitUntilNewOwner(channel, bundle, dstBroker);
        String lookupBroker2 = dstAdmin.lookups().lookupTopic(topic);

        log.info("Topic {} broker2 url: {} after transfer", topic, lookupBroker2);

        assertEquals(dstBrokerLookupAddress, lookupBroker2);
        assertEquals(lookupBroker2, srcAdmin.lookups().lookupTopic(topic));

        // recovery test
        BundleStateChannel dstBundleStateChannel =
                ((ExtensibleLoadManagerWrapper) dstPulsar.getLoadManager().get()).get().getBundleStateChannel();
        LeaderElectionService leaderElectionService = (LeaderElectionService) FieldUtils.readDeclaredField(
                dstBundleStateChannel, "leaderElectionService",  true);
        ((ExtensibleLoadManagerWrapper) dstPulsar.getLoadManager().get()).get().getBrokerRegistry().unregister();
        leaderElectionService.close();;


        waitUntilNewOwner(channel, bundle, null);
        String lookupBroker3 = srcAdmin.lookups().lookupTopic(topic);

        log.info("Topic {} broker3 url: {} after dstPulsar unregistered from zk", topic, lookupBroker3);

        assertEquals(srcBrokerLookupAddress, lookupBroker3);
    }

    @Test
    public void testSplitBundle() throws Exception {
        String topic = "test-split-bundle-topic";
        admin1.topics().createPartitionedTopic(topic, 1);
        String lookupBroker = admin1.lookups().lookupTopic(topic);
        log.info("Topic {} broker url: {}", topic, lookupBroker);
        assertEquals(lookupBroker, admin2.lookups().lookupTopic(topic));
        String broker1LookupServiceAddress = primaryLoadManager.getBrokerRegistry().getLookupServiceAddress();
        String broker2LookupServiceAddress = secondaryLoadManager.getBrokerRegistry().getLookupServiceAddress();

        String bundleRange = admin1.lookups().getBundleRange(topic);
        String bundle = String.format("%s/%s", namespace, bundleRange);

        log.info("The bundle: {}", bundle);
        assertEquals(9, admin1.namespaces().getBundles(namespace).getNumBundles());
        BundleStateChannel channel = secondaryLoadManager.getBundleStateChannel();
        String sourceBroker = broker2LookupServiceAddress;
        Optional<BrokerLookupData> lookup = primaryLoadManager.getBrokerRegistry().lookup(broker1LookupServiceAddress);
        if (lookup.get().getPulsarServiceUrl().equals(lookupBroker)) {
            channel = primaryLoadManager.getBundleStateChannel();
            sourceBroker = broker1LookupServiceAddress;
        }

        channel.splitBundle(new Split(sourceBroker, bundle, Optional.empty())).get();

        Awaitility.await().untilAsserted(() -> {
            String newBundleRange = admin1.lookups().getBundleRange(topic);
            log.info("The new bundle range is {}", newBundleRange);
            assertNotEquals(bundleRange, newBundleRange);
        });
        String bundleRange1 = admin1.lookups().getBundleRange(topic);
        assertNotEquals(bundleRange1, bundleRange);

        assertEquals(admin2.lookups().getBundleRange(topic), bundleRange1);
        assertEquals(10, admin1.namespaces().getBundles(namespace).getNumBundles());
    }


    private void waitUntilNewOwner(BundleStateChannel channel, String key, String target)
            throws IllegalAccessException {
        int max = 10;
        TableViewImpl<BundleStateData> tv = (TableViewImpl<BundleStateData>)
                FieldUtils.readField(channel, "tv", true);
        Awaitility.await()
                .pollInterval(200, TimeUnit.MILLISECONDS)
                .atMost(max, TimeUnit.SECONDS)
                .until(() -> { // wait until true
                    BundleStateData actual = tv.get(key);
                    if (actual == null) {
                        return target == null;
                    } else {
                        return actual.getBroker().equals(target);
                    }
                });
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
