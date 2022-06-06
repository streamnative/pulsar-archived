package org.apache.pulsar.broker.loadbalance.extensible.data;

import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.TableView;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.naming.TopicDomain;
import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.function.BiConsumer;

public class TableViewLoadDataStoreImpl<T> implements LoadDataStore<T> {

    private final TableView<T> tableView;

    private final Producer<T> producer;

    private static final String TOPIC_PREFIX =
            TopicDomain.persistent.value()
                    + "://"
                    + NamespaceName.SYSTEM_NAMESPACE
                    + "/load-data/";

    public TableViewLoadDataStoreImpl(String name, PulsarService pulsar, Schema<T> schema)
            throws LoadDataStoreException {
        try {
            this.tableView = pulsar.getClient()
                    .newTableViewBuilder(schema)
                    .topic(TOPIC_PREFIX + name)
                    .create();
            this.producer = pulsar.getClient().newProducer(schema)
                    .topic(TOPIC_PREFIX + name)
                    .create();
        } catch (Exception e) {
            throw new LoadDataStoreException(e);
        }
    }

    @Override
    public void push(String key, T loadData) throws LoadDataStoreException {
        try {
            this.pushAsync(key, loadData).get();
        } catch (ExecutionException | InterruptedException e) {
            throw new LoadDataStoreException(e);
        }
    }

    @Override
    public CompletableFuture<Void> pushAsync(String key, T loadData) {
        return producer.newMessage().key(key).value(loadData).sendAsync().thenAccept(__ -> {});
    }

    @Override
    public T get(String key) {
        return tableView.get(key);
    }

    @Override
    public CompletableFuture<T> getAsync(String key) {
        return CompletableFuture.completedFuture(tableView.get(key));
    }

    @Override
    public CompletableFuture<Void> removeAsync(String key) {
        return producer.newMessage().key(key).value(null).sendAsync().thenAccept(__ -> {});
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
        tableView.forEach(action);
    }

    @Override
    public void listen(BiConsumer<String, T> listener) {
        tableView.forEachAndListen(listener);
    }

    @Override
    public int size() {
        return tableView.size();
    }

    @Override
    public void close() throws IOException {
        if (producer != null) {
            producer.close();
        }
        if (tableView != null) {
            tableView.close();
        }
    }
}
