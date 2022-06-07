package org.apache.pulsar.broker.loadbalance.extensible.data;

import static org.testng.Assert.assertEquals;
import static org.testng.AssertJUnit.assertNull;
import static org.testng.AssertJUnit.assertTrue;

import lombok.AllArgsConstructor;
import lombok.Cleanup;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.pulsar.broker.auth.MockedPulsarServiceBaseTest;
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

        @Cleanup
        LoadDataStore<MyClass> loadDataStore =
                LoadDataStoreFactory.newInstance(pulsar, impl, UUID.randomUUID().toString(), MyClass.class);
        MyClass myClass1 = new MyClass("1", 1);
        loadDataStore.push("key1", myClass1);

        Awaitility.await().untilAsserted(() -> assertEquals(loadDataStore.get("key1"), myClass1));
        assertEquals(loadDataStore.size(), 1);

        MyClass myClass2 = new MyClass("2", 2);
        loadDataStore.push("key2", myClass2);

        Awaitility.await().untilAsserted(() -> assertEquals(loadDataStore.get("key2"), myClass2));
        assertEquals(loadDataStore.size(), 2);

        loadDataStore.remove("key2");
        Awaitility.await().untilAsserted(() -> assertNull(loadDataStore.get("key2")));
        assertEquals(loadDataStore.size(), 1);

    }

    @Test(dataProvider = "impl")
    public void testForEach(String impl) throws IOException {
        @Cleanup
        LoadDataStore<Integer> loadDataStore =
                LoadDataStoreFactory.newInstance(pulsar, impl, UUID.randomUUID().toString(), Integer.class);

        Map<String, Integer> map = new HashMap<>();
        for (int i = 0; i < 10; i++) {
            String key = "key-" + i;
            Integer value = i;
            loadDataStore.push(key, value);
            map.put(key, value);
        }
        Awaitility.await().untilAsserted(() -> assertEquals(loadDataStore.size(), 10));

        loadDataStore.forEach((key, value) -> {
            assertTrue(map.containsKey(key));
            assertEquals(loadDataStore.get(key), map.get(key));
        });
    }

    @Test(dataProvider = "impl")
    public void testListen(String impl) throws IOException {
        @Cleanup
        LoadDataStore<Integer> loadDataStore =
                LoadDataStoreFactory.newInstance(pulsar, impl, UUID.randomUUID().toString(), Integer.class);

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