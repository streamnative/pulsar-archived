package org.apache.pulsar.broker.loadbalance.extensible.data;

import java.io.Closeable;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiConsumer;

/**
 * The load data store interface.
 *
 * @param <T> The Load data type.
 */
public interface LoadDataStore<T> extends Closeable {

    /**
     * Push load data to store.
     *
     * @param key
     *           The load data key.
     * @param loadData
     *           The load data.
     */
    void push(String key, T loadData) throws LoadDataStoreException;

    /**
     * Async push load data to store.
     *
     * @param key
     *           The load data key.
     * @param loadData
     *           The load data.
     */
    CompletableFuture<Void> pushAsync(String key, T loadData);

    /**
     * Get load data by key.
     *
     * @param key
     *           The load data key.
     */
    T get(String key);

    /**
     * Async get load data by key.
     *
     * @param key
     *           The load data key.
     */
    CompletableFuture<T> getAsync(String key);

    CompletableFuture<Void> removeAsync(String key);

    void remove(String key) throws LoadDataStoreException;

    void forEach(BiConsumer<String, T> action);

    /**
     * Listen the load data change.
     */
    void listen(BiConsumer<String, T> listener);

    /**
     * The load data key count.
     */
    int size();
}
