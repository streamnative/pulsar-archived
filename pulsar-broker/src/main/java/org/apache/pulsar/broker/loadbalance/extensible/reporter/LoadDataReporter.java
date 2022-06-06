package org.apache.pulsar.broker.loadbalance.extensible.reporter;

/**
 * The load data reporter, it report the load data to load data storage.
 *
 * @param <T> load data type.
 */
public interface LoadDataReporter<T> extends AutoCloseable {

    /**
     * Start the {@link LoadDataReporter}.
     */
    void start();

    /**
     * Flush the load data to storage manually.
     */
    void flush();

}
