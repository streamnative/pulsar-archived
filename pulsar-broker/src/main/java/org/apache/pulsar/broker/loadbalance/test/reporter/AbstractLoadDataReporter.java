package org.apache.pulsar.broker.loadbalance.test.reporter;

import io.netty.util.concurrent.DefaultThreadFactory;
import org.apache.pulsar.broker.loadbalance.test.data.LoadDataStore;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

/**
 * Abstract load data reporter.
 *
 * @param <T> Load data type.
 */
public abstract class AbstractLoadDataReporter<T> implements LoadDataReporter<T> {

    private final Map<String, T> loadDataMap;

    private final ScheduledExecutorService executor;

    protected AbstractLoadDataReporter() {
        this.loadDataMap = new HashMap<>();
        this.executor = Executors
                .newSingleThreadScheduledExecutor(new DefaultThreadFactory("load-data-reporter"));
    }

    abstract LoadDataStore<T> getLoadDataStore();

    abstract Map<String, T> generateLoadData();

    @Override
    public void flush() {
        loadDataMap.forEach((key, loadData) -> this.getLoadDataStore().push(key, loadData));
    }

}
