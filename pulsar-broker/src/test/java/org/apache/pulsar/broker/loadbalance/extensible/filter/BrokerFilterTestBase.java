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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiConsumer;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.loadbalance.extensible.BaseLoadManagerContext;
import org.apache.pulsar.broker.loadbalance.extensible.BaseLoadManagerContextImpl;
import org.apache.pulsar.broker.loadbalance.extensible.BrokerLookupData;
import org.apache.pulsar.broker.loadbalance.extensible.BrokerRegistry;
import org.apache.pulsar.broker.loadbalance.extensible.data.LoadDataStore;
import org.apache.pulsar.broker.loadbalance.extensible.data.LoadDataStoreException;

import static org.testng.Assert.assertTrue;

public class BrokerFilterTestBase {

    public BaseLoadManagerContext newBaseLoadManagerContext() {
        BaseLoadManagerContextImpl context = new BaseLoadManagerContextImpl();
        context.setTimeAverageBrokerLoadDataStore(new MockedLoadDataStore<>());
        context.setBrokerLoadDataStore(new MockedLoadDataStore<>());
        context.setBundleLoadDataStore(new MockedLoadDataStore<>());
        context.setConfiguration(new ServiceConfiguration());
        context.setBrokerRegistry(new MockedBrokerRegistry());
        return context;
    }

    public void registerBroker(BrokerRegistry registry, String broker, BrokerLookupData lookupData) {
        assertTrue(registry instanceof MockedBrokerRegistry);
        ((MockedBrokerRegistry) registry).brokerLookupDataMap.put(broker, lookupData);
    }

    public void unregisterBroker(BrokerRegistry registry, String broker) {
        assertTrue(registry instanceof MockedBrokerRegistry);
        ((MockedBrokerRegistry) registry).brokerLookupDataMap.remove(broker);
    }

    static class MockedBrokerRegistry implements BrokerRegistry {

        private final Map<String, BrokerLookupData> brokerLookupDataMap = new ConcurrentHashMap<>();

        private final String mockedLookupServiceAddress = "mocked-lookup-service-address";

        @Override
        public void start() {
            // No-op
        }

        @Override
        public void register() {
            // No-op
        }

        @Override
        public void unregister() {
            // No-op
        }

        @Override
        public String getLookupServiceAddress() {
            return this.mockedLookupServiceAddress;
        }

        @Override
        public List<String> getAvailableBrokers() {
            return new ArrayList<>(brokerLookupDataMap.keySet());
        }

        @Override
        public CompletableFuture<List<String>> getAvailableBrokersAsync() {
            return CompletableFuture.completedFuture(new ArrayList<>(brokerLookupDataMap.keySet()));
        }

        @Override
        public Optional<BrokerLookupData> lookup(String broker) {
            return Optional.ofNullable(brokerLookupDataMap.get(broker));
        }

        @Override
        public void forEach(BiConsumer<String, BrokerLookupData> action) {
            brokerLookupDataMap.forEach(action);
        }

        @Override
        public void close() throws Exception {

        }
    }


    static class MockedLoadDataStore<T> implements LoadDataStore<T> {

        private final Map<String, T> store = new ConcurrentHashMap<>();

        @Override
        public void push(String key, T loadData) throws LoadDataStoreException {
            store.put(key, loadData);
        }

        @Override
        public CompletableFuture<Void> pushAsync(String key, T loadData) {
            store.put(key, loadData);
            return CompletableFuture.completedFuture(null);
        }

        @Override
        public Optional<T> get(String key) {
            return Optional.empty();
        }

        @Override
        public CompletableFuture<Optional<T>> getAsync(String key) {
            return CompletableFuture.completedFuture(Optional.ofNullable(store.get(key)));
        }

        @Override
        public CompletableFuture<Void> removeAsync(String key) {
            store.remove(key);
            return CompletableFuture.completedFuture(null);
        }

        @Override
        public void remove(String key) throws LoadDataStoreException {
            store.remove(key);
        }

        @Override
        public void forEach(BiConsumer<String, T> action) {
            store.forEach(action);
        }

        @Override
        public void listen(BiConsumer<String, T> listener) {

        }

        @Override
        public int size() {
            return store.size();
        }

        @Override
        public void close() throws IOException {

        }
    }

}
