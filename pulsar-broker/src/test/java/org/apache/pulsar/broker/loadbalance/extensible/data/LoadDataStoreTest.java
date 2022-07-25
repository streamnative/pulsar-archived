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
package org.apache.pulsar.broker.loadbalance.extensible.data;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.AssertJUnit.assertTrue;

import com.google.common.collect.Sets;
import lombok.AllArgsConstructor;
import lombok.Cleanup;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.pulsar.broker.auth.MockedPulsarServiceBaseTest;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.naming.TopicDomain;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.common.policies.data.TenantInfoImpl;
import org.awaitility.Awaitility;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

/**
 * The {@link LoadDataStore} unit test.
 */
public class LoadDataStoreTest extends MockedPulsarServiceBaseTest {

    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    static class MyClass {
        String a;
        int b;
    }

    @BeforeClass
    @Override
    protected void setup() throws Exception {
        super.internalSetup();
    }

    @AfterClass
    @Override
    protected void cleanup() throws Exception {
        super.internalCleanup();
    }

    @DataProvider(name = "impl")
    public Object[] implementations() {
        return new Object[]{
                LoadDataStoreFactory.TABLEVIEW_STORE
        };
    }

    @Test(dataProvider = "impl")
    public void testPushGetAndRemove(String impl) throws IOException {

        String topic = TopicDomain.persistent + "://" + NamespaceName.SYSTEM_NAMESPACE + "/" + UUID.randomUUID();

        @Cleanup
        LoadDataStore<MyClass> loadDataStore =
                LoadDataStoreFactory.newInstance(pulsar, impl, topic, MyClass.class);
        MyClass myClass1 = new MyClass("1", 1);
        loadDataStore.push("key1", myClass1);

        Awaitility.await().untilAsserted(() -> {
            assertTrue(loadDataStore.get("key1").isPresent());
            assertEquals(loadDataStore.get("key1").get(), myClass1);
        });
        assertEquals(loadDataStore.size(), 1);

        MyClass myClass2 = new MyClass("2", 2);
        loadDataStore.push("key2", myClass2);

        Awaitility.await().untilAsserted(() -> {
            assertTrue(loadDataStore.get("key2").isPresent());
            assertEquals(loadDataStore.get("key2").get(), myClass2);
        });
        assertEquals(loadDataStore.size(), 2);

        loadDataStore.remove("key2");
        Awaitility.await().untilAsserted(() -> assertFalse(loadDataStore.get("key2").isPresent()));
        assertEquals(loadDataStore.size(), 1);

    }

    @Test(dataProvider = "impl")
    public void testForEach(String impl) throws IOException {

        String topic = TopicDomain.persistent + "://" + NamespaceName.SYSTEM_NAMESPACE + "/" + UUID.randomUUID();

        @Cleanup
        LoadDataStore<Integer> loadDataStore =
                LoadDataStoreFactory.newInstance(pulsar, impl, topic, Integer.class);

        Map<String, Integer> map = new HashMap<>();
        for (int i = 0; i < 10; i++) {
            String key = "key-" + i;
            Integer value = i;
            loadDataStore.push(key, value);
            map.put(key, value);
        }
        Awaitility.await().untilAsserted(() -> assertEquals(loadDataStore.size(), 10));

        loadDataStore.forEach((key, value) -> {
            assertTrue(loadDataStore.get(key).isPresent());
            assertEquals(loadDataStore.get(key).get(), map.get(key));
        });
    }

    @Test(dataProvider = "impl")
    public void testListen(String impl) throws IOException {

        String topic = TopicDomain.persistent + "://" + NamespaceName.SYSTEM_NAMESPACE + "/" + UUID.randomUUID();

        @Cleanup
        LoadDataStore<Integer> loadDataStore =
                LoadDataStoreFactory.newInstance(pulsar, impl, topic, Integer.class);

        Map<String, Integer> map = new ConcurrentHashMap<>();
        loadDataStore.listen(map::put);

        for (int i = 0; i < 10; i++) {
            String key = "key-" + i;
            loadDataStore.push(key, i);
        }

        Awaitility.await().untilAsserted(() -> assertEquals(loadDataStore.size(), 10));

        assertEquals(map.size(), loadDataStore.size());
    }

}