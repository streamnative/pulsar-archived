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


import static org.apache.pulsar.broker.loadbalance.extensible.channel.BundleStateChannel.MAX_CLEAN_UP_DELAY_TIME_IN_SECS;
import static org.apache.pulsar.metadata.api.extended.SessionEvent.SessionLost;
import static org.apache.pulsar.metadata.api.extended.SessionEvent.SessionReestablished;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import java.net.URL;
import java.util.Collections;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
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
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.impl.TableViewImpl;
import org.apache.pulsar.common.naming.NamespaceBundle;
import org.apache.pulsar.common.naming.NamespaceBundleFactory;
import org.apache.pulsar.common.policies.data.BundlesData;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.common.policies.data.Policies;
import org.apache.pulsar.common.policies.data.TenantInfo;
import org.apache.pulsar.common.util.ObjectMapperFactory;
import org.apache.pulsar.common.util.collections.ConcurrentOpenHashMap;
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

    private PulsarClient pulsarClient;

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

        pulsarClient = PulsarClient.builder()
                .serviceUrl(pulsar1.getBrokerServiceUrl())
                .statsInterval(0, TimeUnit.SECONDS).build();
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
        PulsarAdmin dstAdmin = admin1;
        String srcBroker = broker2LookupServiceAddress;
        PulsarAdmin srcAdmin = admin2;

        if(pulsar1.getBrokerServiceUrl().equals(lookupBroker)){
            dstBroker = broker2LookupServiceAddress;
            dstBrokerLookupAddress = pulsar2.getBrokerServiceUrl();
            dstAdmin = admin2;
            srcBroker = broker1LookupServiceAddress;
            srcAdmin = admin1;
        }
        Unload unload = new Unload(srcBroker, bundle, Optional.of(dstBroker));
        channel.publishUnload(unload);
        waitUntilNewOwner(channel, bundle, dstBroker);
        String lookupBroker2 = dstAdmin.lookups().lookupTopic(topic);

        log.info("Topic {} broker2 url: {} after transfer", topic, lookupBroker2);

        assertEquals(dstBrokerLookupAddress, lookupBroker2);
        assertEquals(lookupBroker2, srcAdmin.lookups().lookupTopic(topic));

    }

    @Test
    public void testBundleStateChannelRecovery()
            throws Exception {
        String topic = "test-recovery-stable";
        admin1.topics().createPartitionedTopic(topic, 1);
        String lookupBroker = admin1.lookups().lookupTopic(topic);
        BrokerRegistry srcBrokerRegistry = primaryLoadManager.getBrokerRegistry();
        LeaderElectionService srcLeaderElectionService = (LeaderElectionService) FieldUtils.readDeclaredField(
                primaryLoadManager.getBundleStateChannel(), "leaderElectionService",  true);
        BundleStateChannel srcBundleStateChannel = primaryLoadManager.getBundleStateChannel();
        String dstBrokerLookupAddress = pulsar2.getBrokerServiceUrl();
        BundleStateChannel dstBundleStateChannel = secondaryLoadManager.getBundleStateChannel();
        PulsarAdmin dstAdmin = admin2;

        if (pulsar2.getBrokerServiceUrl().equals(lookupBroker)) {
            srcBrokerRegistry = secondaryLoadManager.getBrokerRegistry();
            srcBundleStateChannel = secondaryLoadManager.getBundleStateChannel();
            srcLeaderElectionService = (LeaderElectionService) FieldUtils.readDeclaredField(
                    srcBundleStateChannel, "leaderElectionService", true);
            dstBrokerLookupAddress = pulsar1.getBrokerServiceUrl();
            dstBundleStateChannel = primaryLoadManager.getBundleStateChannel();

            dstAdmin = admin1;
        }

        String bundle = String.format("%s/%s", namespace, admin1.lookups().getBundleRange(topic));
        log.info("bundle: {}", bundle);

        final BundleStateChannel leaderChannel = srcLeaderElectionService.isLeader() ?
                srcBundleStateChannel : dstBundleStateChannel;
        ConcurrentOpenHashMap<String, CompletableFuture<Optional<String>>> cleanupJobs =
                (ConcurrentOpenHashMap<String, CompletableFuture<Optional<String>>>)
                        FieldUtils.readDeclaredField(leaderChannel, "cleanupJobs",  true);


        // When the metadata session is unstable, and when a broker unregisters and then soon registers again,
        // the bundle state channel does not trigger a clean-up job.
        leaderChannel.handleMetadataSessionEvent(SessionLost);
        srcBrokerRegistry.unregister();

        Awaitility.await()
                .pollInterval(200, TimeUnit.MILLISECONDS)
                .atMost(10, TimeUnit.SECONDS)
                .until(() -> getCleanUpStat(leaderChannel, "totalIgnoredCleanUpCnt") == 1);

        assertEquals(0, cleanupJobs.size());
        assertEquals(0, getCleanUpStat(leaderChannel, "totalCleanedBundleCnt"));
        assertEquals(0, getCleanUpStat(leaderChannel, "totalCleanedBrokerCnt"));
        assertEquals(1, getCleanUpStat(leaderChannel, "totalIgnoredCleanUpCnt"));

        srcBrokerRegistry.register();

        // When the metadata session is jittery, and when a broker unregisters and then soon registers again,
        // the bundle state channel cancels the scheduled the clean-up job.
        leaderChannel.handleMetadataSessionEvent(SessionReestablished);
        srcBrokerRegistry.unregister();

        Awaitility.await()
                .pollInterval(200, TimeUnit.MILLISECONDS)
                .atMost(10, TimeUnit.SECONDS)
                .until(() -> cleanupJobs.size() > 0);

        srcBrokerRegistry.register();

        Awaitility.await()
                .pollInterval(200, TimeUnit.MILLISECONDS)
                .atMost(10, TimeUnit.SECONDS)
                .until(() -> cleanupJobs.size() == 0);

        assertEquals(0, cleanupJobs.size());
        assertEquals(0, getCleanUpStat(leaderChannel, "totalCleanedBundleCnt"));
        assertEquals(0, getCleanUpStat(leaderChannel, "totalCleanedBrokerCnt"));
        assertEquals(1, getCleanUpStat(leaderChannel, "totalIgnoredCleanUpCnt"));


        // When the metadata session is stable, and when a broker unregisters,
        // the bundle state channel runs a clean-up job.
        FieldUtils.writeDeclaredField(
                srcBundleStateChannel, "lastMetadataSessionEvent",  SessionReestablished, true);
        FieldUtils.writeDeclaredField(srcBundleStateChannel, "lastMetadataSessionEventTimestamp",
                System.currentTimeMillis() - (MAX_CLEAN_UP_DELAY_TIME_IN_SECS * 1000 + 10), true);
        FieldUtils.writeDeclaredField(
                dstBundleStateChannel, "lastMetadataSessionEvent",  SessionReestablished, true);
        FieldUtils.writeDeclaredField(dstBundleStateChannel, "lastMetadataSessionEventTimestamp",
                System.currentTimeMillis() - (MAX_CLEAN_UP_DELAY_TIME_IN_SECS * 1000 + 10), true);

        srcLeaderElectionService.close();
        srcBrokerRegistry.unregister();


        waitUntilNewOwner(dstBundleStateChannel, bundle, null);

        String lookupBroker2 = dstAdmin.lookups().lookupTopic(topic);
        long srcTotalCleanedBundleCnt = getCleanUpStat(srcBundleStateChannel, "totalCleanedBundleCnt");
        long dstTotalCleanedBundleCnt = getCleanUpStat(dstBundleStateChannel, "totalCleanedBundleCnt");
        long srcTotalCleanedBrokerCnt = getCleanUpStat(srcBundleStateChannel, "totalCleanedBrokerCnt");
        long dstTotalCleanedBrokerCnt = getCleanUpStat(dstBundleStateChannel, "totalCleanedBrokerCnt");
        long srcTotalIgnoredCleanUpCnt = getCleanUpStat(srcBundleStateChannel, "totalIgnoredCleanUpCnt");
        long dstTotalIgnoredCleanUpCnt = getCleanUpStat(dstBundleStateChannel, "totalIgnoredCleanUpCnt");
        ConcurrentOpenHashMap<String, CompletableFuture<Optional<String>>> srcCleanupJobs =
                (ConcurrentOpenHashMap<String, CompletableFuture<Optional<String>>>)
                        FieldUtils.readDeclaredField(srcBundleStateChannel, "cleanupJobs",  true);
        ConcurrentOpenHashMap<String, CompletableFuture<Optional<String>>> dstCleanupJobs =
                (ConcurrentOpenHashMap<String, CompletableFuture<Optional<String>>>)
                        FieldUtils.readDeclaredField(dstBundleStateChannel, "cleanupJobs",  true);

        assertEquals(dstBrokerLookupAddress, lookupBroker2);
        assertTrue(srcCleanupJobs.size() == 0 && dstCleanupJobs.size() == 0);

        if (srcTotalCleanedBrokerCnt == 1) {
            assertTrue(srcTotalCleanedBrokerCnt == 1);
            assertTrue(srcTotalCleanedBundleCnt > 0);
            assertTrue(dstTotalCleanedBrokerCnt == 0);
            assertTrue(dstTotalCleanedBundleCnt == 0);

        } else {
            assertTrue(srcTotalCleanedBrokerCnt == 0);
            assertTrue(srcTotalCleanedBundleCnt == 0);
            assertTrue(dstTotalCleanedBrokerCnt == 1);
            assertTrue(dstTotalCleanedBundleCnt > 0);
        }

        if (srcTotalIgnoredCleanUpCnt == 1) {
            assertTrue(dstTotalIgnoredCleanUpCnt == 0);
        } else {
            assertTrue(dstTotalIgnoredCleanUpCnt == 1);
        }
    }

    @Test
    public void testUnloadBundleWithAdminApi() throws PulsarAdminException, IllegalAccessException {
        String topic = "test";
        admin1.topics().createPartitionedTopic(topic, 1);
        String lookupBroker = admin1.lookups().lookupTopic(topic);
        log.info("Topic {} broker url: {}", topic, lookupBroker);
        assertEquals(lookupBroker, admin2.lookups().lookupTopic(topic));

        String broker1LookupServiceAddress = primaryLoadManager.getBrokerRegistry().getLookupServiceAddress();
        String broker2LookupServiceAddress = secondaryLoadManager.getBrokerRegistry().getLookupServiceAddress();


        // bundle state channel tests
        // basic transfer test
        String bundleRange = admin1.lookups().getBundleRange(topic);
        String bundle = String.format("%s/%s", namespace, bundleRange);
        log.info("bundle: {}", bundle);
        BundleStateChannel channel = primaryLoadManager.getBundleStateChannel();

        String dstBrokerLookupAddress = pulsar1.getBrokerServiceUrl();
        String dstBroker = broker1LookupServiceAddress;
        PulsarAdmin dstAdmin = admin1;
        PulsarAdmin srcAdmin = admin2;

        if(pulsar1.getBrokerServiceUrl().equals(lookupBroker)){
            dstBrokerLookupAddress = pulsar2.getBrokerServiceUrl();
            dstBroker = broker2LookupServiceAddress;
            dstAdmin = admin2;
            srcAdmin = admin1;
        }
        admin2.namespaces().unloadNamespaceBundle(namespace, bundleRange, dstBroker);
        waitUntilNewOwner(channel, bundle, dstBroker);
        String lookupBroker2 = dstAdmin.lookups().lookupTopic(topic);

        log.info("Topic {} broker2 url: {} after transfer", topic, lookupBroker2);

        assertEquals(dstBrokerLookupAddress, lookupBroker2);
        assertEquals(lookupBroker2, srcAdmin.lookups().lookupTopic(topic));

    }

    @Test
    public void testNamespaceUnloadBundle() throws Exception {

        String topic = "testNamespaceUnloadBundle";
        String fullTopicName = "persistent://" + namespace + "/"+ topic;
        admin1.topics().createPartitionedTopic(topic, 1);

        String bundleRange = admin1.lookups().getBundleRange(fullTopicName);
        String brokerUrl = admin1.lookups().lookupTopic(topic);

        log.info("The bundle range is {} brokerUrl {}", bundleRange, brokerUrl);

        NamespaceBundleFactory bundleFactory = new NamespaceBundleFactory(pulsar1, Hashing.crc32());
        NamespaceBundle bundle = bundleFactory.getBundle(namespace, bundleRange);
        assertTrue(pulsar1.getNamespaceService().isServiceUnitOwned(bundle)
                || pulsar2.getNamespaceService().isServiceUnitOwned(bundle));

        try {
            admin1.namespaces().unloadNamespaceBundle(namespace, bundleRange);
        } catch (Exception e) {
            log.error("Unload throw exception", e);
            fail("Unload shouldn't have throw exception");
        }

        // check that no one owns the namespace
        Awaitility.await().untilAsserted(() -> {
            assertFalse(pulsar1.getNamespaceService().isServiceUnitOwned(bundle));
            assertFalse(pulsar2.getNamespaceService().isServiceUnitOwned(bundle));
        });

        Awaitility.await().untilAsserted(() -> {
            String newBundleRange = admin1.lookups().getBundleRange(topic);
            String url = admin1.lookups().lookupTopic(topic);
            log.info("The new bundle range is {} brokerUrl {}", newBundleRange, url);
            assertTrue(pulsar1.getNamespaceService().isServiceUnitOwned(bundle)
                    || pulsar2.getNamespaceService().isServiceUnitOwned(bundle));
        });

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
                    log.info("WaitUntilNewOwner BundleStateData: {} : {}", key, actual);
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

    private static long getCleanUpStat(BundleStateChannel channel, String metric)
            throws IllegalAccessException {
        return (long) FieldUtils.readDeclaredField(channel, metric, true);
    }
}
