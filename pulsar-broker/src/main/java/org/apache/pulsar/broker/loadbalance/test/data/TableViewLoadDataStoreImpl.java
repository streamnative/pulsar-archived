package org.apache.pulsar.broker.loadbalance.test.data;

import java.util.concurrent.CompletableFuture;
import java.util.function.BiConsumer;

public class TableViewLoadDataStoreImpl<T> implements LoadDataStore<T> {

    @Override
    public void start() {

    }

    @Override
    public void push(String key, T loadData) {

    }

    @Override
    public CompletableFuture<Void> pushAsync(String key, T loadData) {
        return null;
    }

    @Override
    public T get(String key) {
        return null;
    }

    @Override
    public CompletableFuture<T> getAsync(String key) {
        return null;
    }

    @Override
    public CompletableFuture<Void> removeAsync(String key) {
        return null;
    }

    @Override
    public void remove(String key) {

    }

    @Override
    public void forEach(BiConsumer<String, T> action) {

    }

    @Override
    public void listen(BiConsumer<String, T> listener) {

    }

    @Override
    public int size() {
        return -1;
    }

    @Override
    public void stop() {

    }
}
