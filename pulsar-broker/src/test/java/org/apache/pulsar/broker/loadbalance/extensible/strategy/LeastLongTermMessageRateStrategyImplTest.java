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
package org.apache.pulsar.broker.loadbalance.extensible.strategy;

import static org.testng.Assert.assertTrue;
import static org.testng.Assert.assertEquals;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import org.apache.pulsar.broker.loadbalance.extensible.BaseLoadManagerContext;
import org.apache.pulsar.broker.loadbalance.extensible.ExtensibleLoadManagerTestBase;
import org.apache.pulsar.broker.loadbalance.extensible.data.BrokerLoadData;
import org.apache.pulsar.broker.loadbalance.extensible.data.LoadDataStoreException;
import org.apache.pulsar.policies.data.loadbalancer.ResourceUsage;
import org.apache.pulsar.policies.data.loadbalancer.TimeAverageBrokerData;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * The unit test for {@link LeastLongTermMessageRateStrategyImpl}.
 */
public class LeastLongTermMessageRateStrategyImplTest extends ExtensibleLoadManagerTestBase {

    private LeastLongTermMessageRateStrategyImpl strategy;

    private BaseLoadManagerContext context;

    @BeforeMethod
    public void setUp() {
        strategy = new LeastLongTermMessageRateStrategyImpl();
        context = newBaseLoadManagerContext();
    }

    @AfterMethod
    public void tearDown() {
        strategy = null;
        context = null;
    }

    @Test
    public void testEmptyBroker() {
        Optional<String> selectedBroker = strategy.select(new ArrayList<>(), context);

        Assert.assertFalse(selectedBroker.isPresent());
    }

    @Test
    public void testNoTimeAverageBrokerData() {
        List<String> brokers = new ArrayList<>(Arrays.asList("broker-1", "broker-2"));

        Optional<String> selectedBroker = strategy.select(brokers, context);

        assertTrue(selectedBroker.isPresent());
    }

    @Test
    public void testLeastLongTermMessageRate() throws LoadDataStoreException {
        TimeAverageBrokerData timeAverageBrokerData1 = new TimeAverageBrokerData();
        timeAverageBrokerData1.setLongTermMsgRateIn(100);
        context.timeAverageBrokerLoadDataStore().push("broker-1", timeAverageBrokerData1);
        TimeAverageBrokerData timeAverageBrokerData2 = new TimeAverageBrokerData();
        timeAverageBrokerData2.setLongTermMsgRateIn(200);
        context.timeAverageBrokerLoadDataStore().push("broker-2", timeAverageBrokerData2);
        TimeAverageBrokerData timeAverageBrokerData3 = new TimeAverageBrokerData();
        timeAverageBrokerData3.setLongTermMsgRateIn(300);
        context.timeAverageBrokerLoadDataStore().push("broker-3", timeAverageBrokerData3);

        List<String> brokers = new ArrayList<>(Arrays.asList("broker-1", "broker-2", "broker-3"));

        // Test select the least long term message rate.
        assertEquals(strategy.select(brokers, context), Optional.of("broker-1"));

        // Make broker-1 message rate maximum, so we should select broker-2.
        timeAverageBrokerData1.setLongTermMsgRateIn(400);
        assertEquals(strategy.select(brokers, context), Optional.of("broker-2"));

        // Make broker-2 CPU overload, so we should select broker-3
        int overloadedThresholdPercentage =
                context.brokerConfiguration().getLoadBalancerBrokerOverloadedThresholdPercentage();

        BrokerLoadData brokerLoadData = newBrokerLoadData();
        brokerLoadData.setCpu(
                new ResourceUsage(overloadedThresholdPercentage + 1, 100));
        context.brokerLoadDataStore().push("broker-2", brokerLoadData);

        assertEquals(strategy.select(brokers, context), Optional.of("broker-3"));
    }
}