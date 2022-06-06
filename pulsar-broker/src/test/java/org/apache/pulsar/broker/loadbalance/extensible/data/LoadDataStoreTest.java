package org.apache.pulsar.broker.loadbalance.extensible.data;

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

import java.util.UUID;

import static org.testng.Assert.*;

public class LoadDataStoreTest extends MockedPulsarServiceBaseTest {

    private static final String NAME = "test";

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
    public void testPushGetAndRemove(String impl) throws LoadDataStoreException {

        LoadDataStore<MyClass> loadDataStore =
                LoadDataStoreFactory.newInstance(pulsar, impl, "test", MyClass.class);
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


    @Test
    public void testRemove() {
    }

    @Test
    public void testForEach() {
    }

    @Test
    public void testListen() {
    }

    @Test
    public void testSize() {
    }

}