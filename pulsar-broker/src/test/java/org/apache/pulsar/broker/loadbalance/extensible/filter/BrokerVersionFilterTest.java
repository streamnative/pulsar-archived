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
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import com.github.zafarkhaja.semver.Version;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.broker.loadbalance.BrokerFilterException;
import org.apache.pulsar.broker.loadbalance.extensible.BaseLoadManagerContext;
import org.apache.pulsar.broker.loadbalance.extensible.BrokerLookupData;
import org.apache.pulsar.broker.loadbalance.extensible.BrokerRegistry;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * The unit test for {@link BrokerVersionFilter}.
 */
@Slf4j
public class BrokerVersionFilterTest extends BrokerFilterTestBase {

    private BrokerVersionFilter filter;

    private BaseLoadManagerContext context;

    @BeforeMethod
    public void setUp() {
        filter = new BrokerVersionFilter();
        context = newBaseLoadManagerContext();
        context.brokerConfiguration().setPreferLaterVersions(true);
    }

    @AfterMethod
    public void tearDown() {
        filter = null;
        context = null;
    }

    @Test
    public void testNoVersionStringInLookupData() {
        List<String> brokers = new ArrayList<>(Arrays.asList("broker-1", "broker-2"));

        BrokerRegistry brokerRegistry = context.brokerRegistry();
        registerBroker(brokerRegistry, "broker-1", new BrokerLookupData());
        try {
            filter.filter(brokers, context);
            fail("Should fail with BrokerFilterException.");
        } catch (BrokerFilterException ex) {
            log.info("The broker filter throws an exception.", ex);
            assertTrue(ex.getMessage().contains("No version string in lookup data for broker"));
        }
    }

    @Test
    public void testNullBrokers() {
        try {
            filter.filter(null, context);
            fail("Should fail with BrokerFilterException.");
        } catch (BrokerFilterException ex) {
            log.info("The broker filter throws an exception.", ex);
            assertTrue(ex.getMessage().contains("Unable to determine latest version since broker set was null"));
        }
    }

    @Test
    public void testDisablePreferLaterVersionsSetting() throws BrokerFilterException {
        context.brokerConfiguration().setPreferLaterVersions(false);
        filter.filter(null, context);
    }

    @Test
    public void testEmptyBrokerList() {
        try {
            filter.filter(new ArrayList<>(), context);
            fail("Should fail with BrokerFilterException.");
        } catch (BrokerFilterException ex) {
            log.info("The broker filter throws an exception.", ex);
            assertTrue(ex.getMessage().contains("Unable to determine latest version since broker set was empty"));
        }
    }

    @Test
    public void testFilterOutOldVersionBroker() throws BrokerFilterException {
        List<String> brokers = new ArrayList<>(Arrays.asList("broker-1", "broker-2"));

        BrokerRegistry brokerRegistry = context.brokerRegistry();
        BrokerLookupData brokerLookupData1 = new BrokerLookupData();
        brokerLookupData1.setBrokerVersion(Version.forIntegers(2, 9, 0).toString());
        registerBroker(brokerRegistry, "broker-1", brokerLookupData1);
        BrokerLookupData brokerLookupData2 = new BrokerLookupData();
        brokerLookupData2.setBrokerVersion(Version.forIntegers(2, 8, 0).toString());
        registerBroker(brokerRegistry, "broker-2", brokerLookupData2);

        filter.filter(brokers, context);

        log.info("Broker candidates: {}", brokers);

        assertEquals(brokers.size(), 1);
        assertEquals(brokers.get(0), "broker-1");
    }

}