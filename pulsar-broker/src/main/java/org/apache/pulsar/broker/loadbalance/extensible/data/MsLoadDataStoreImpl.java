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

import java.io.IOException;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.function.BiConsumer;
import org.apache.pulsar.metadata.api.MetadataCache;
import org.apache.pulsar.metadata.api.Notification;
import org.apache.pulsar.metadata.api.extended.MetadataStoreExtended;

/**
 * The load data storage, backend is MetadataStore.
 *
 * @param <T> Load data type.
 */
public class MsLoadDataStoreImpl<T> implements LoadDataStore<T> {

    public static final String BASE_LOAD_DATA_PATH = "/loadbalance/load-data/";

    private final String path;

    private final MetadataStoreExtended metadataStore;

    private final MetadataCache<T> metadataCache;

    private final Map<String, T> map;

    public MsLoadDataStoreImpl(MetadataStoreExtended metadataStore, String name, Class<T> clazz) {
        this.path = BASE_LOAD_DATA_PATH + "/" + name;
        this.metadataStore = metadataStore;
        this.metadataCache = metadataStore.getMetadataCache(clazz);
//        metadataStore.registerListener();
        this.map = new ConcurrentHashMap<>();
    }

    @Override
    public void push(String key, T loadData) throws LoadDataStoreException {
        try {
            pushAsync(key, loadData).get();
        } catch (InterruptedException | ExecutionException e) {
            throw new LoadDataStoreException(e);
        }
    }

    @Override
    public CompletableFuture<Void> pushAsync(String key, T loadData) {
        return metadataCache.readModifyUpdateOrCreate(path(key), __ -> loadData).thenApply(__ -> null);
    }

    @Override
    public Optional<T> get(String key) {
        return metadataCache.getIfCached(path(key));
    }

    @Override
    public CompletableFuture<Optional<T>> getAsync(String key) {
        return metadataCache.get(path(key));
    }

    @Override
    public CompletableFuture<Void> removeAsync(String key) {
        return metadataCache.delete(path(key));
    }

    @Override
    public void remove(String key) throws LoadDataStoreException {
        try {
            removeAsync(key).get();
        } catch (InterruptedException | ExecutionException e) {
            throw new LoadDataStoreException(e);
        }
    }

    @Override
    public void forEach(BiConsumer<String, T> action) {
        map.forEach(action);
    }

    @Override
    public void listen(BiConsumer<String, T> listener) {

    }

    @Override
    public int size() {
        return -1;
    }

    @Override
    public void close() throws IOException {
    }

    private void handleNotification(Notification notification) {
//        if (notification.getPath().startsWith(path)) {
//            metadataStore.getChildren(path).thenAccept(childrenList -> {
//                for (String children : childrenList) {
//                    metadataCache.get(children).thena
//                }
//            })
//        }
    }

    private String path(String key) {
        return String.format(path + "/" + "%s", key);
    }


}
