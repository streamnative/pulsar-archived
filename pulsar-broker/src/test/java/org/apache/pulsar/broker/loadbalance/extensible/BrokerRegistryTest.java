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

import org.apache.bookkeeper.util.ZkUtils;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.common.util.ObjectMapperFactory;
import org.apache.pulsar.zookeeper.LocalBookkeeperEnsemble;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class BrokerRegistryTest {

    private ExecutorService executor;

    private LocalBookkeeperEnsemble bkEnsemble;

    private PulsarService pulsar1;

    private PulsarService pulsar2;

    @BeforeMethod
    void setup() throws Exception {
        executor = new ThreadPoolExecutor(5, 20, 30, TimeUnit.SECONDS,
                new LinkedBlockingQueue<>());
        // Start local bookkeeper ensemble
        bkEnsemble = new LocalBookkeeperEnsemble(3, 0, () -> 0);
        bkEnsemble.start();

        // Start broker 1
        ServiceConfiguration config1 = new ServiceConfiguration();
        config1.setLoadBalancerEnabled(false);
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
        config2.setLoadBalancerEnabled(false);
        config2.setLoadManagerClassName(BrokerDiscoveryImpl.class.getName());
        config2.setClusterName("use");
        config2.setWebServicePort(Optional.of(0));
        config2.setMetadataStoreUrl("zk:127.0.0.1" + ":" + bkEnsemble.getZookeeperPort());
        config2.setBrokerShutdownTimeoutMs(0L);
        config2.setBrokerServicePort(Optional.of(0));
        config2.setAdvertisedAddress("localhost");
        pulsar2 = new PulsarService(config2);
        pulsar2.start();
    }

    @AfterMethod(alwaysRun = true)
    void shutdown() throws Exception {
        executor.shutdownNow();

        pulsar2.close();
        pulsar1.close();

        bkEnsemble.stop();
    }

    @Test(timeOut = 30 * 1000)
    public void testRegister() throws Exception {
        BrokerRegistry brokerRegistry1 = new BrokerRegistryImpl(pulsar1);
        BrokerRegistry brokerRegistry2 = new BrokerRegistryImpl(pulsar2);
        brokerRegistry1.start();
        brokerRegistry2.start();
        brokerRegistry1.register();
        brokerRegistry2.register();

        Assert.assertEquals(brokerRegistry1.getAvailableBrokers().size(), 2);
        Assert.assertEquals(brokerRegistry2.getAvailableBrokers().size(), 2);

        Optional<BrokerLookupData> lookupDataOpt = brokerRegistry1.lookup(brokerRegistry2.getLookupServiceAddress());
        Assert.assertTrue(lookupDataOpt.isPresent());
        Assert.assertEquals(lookupDataOpt.get().getBrokerVersion(), pulsar2.getBrokerVersion());

        brokerRegistry1.unregister();
        Assert.assertEquals(brokerRegistry2.getAvailableBrokers().size(), 1);

        brokerRegistry1.close();
        brokerRegistry2.close();
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
