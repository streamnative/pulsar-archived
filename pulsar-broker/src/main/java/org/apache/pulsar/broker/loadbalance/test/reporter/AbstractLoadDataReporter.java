package org.apache.pulsar.broker.loadbalance.test.reporter;

import io.netty.util.concurrent.DefaultThreadFactory;
import org.apache.pulsar.broker.loadbalance.test.data.LoadDataStore;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

/**
 * Abstract load data reporter.
 *
 * @param <T> Load data type.
 */
public abstract class AbstractLoadDataReporter<T> implements LoadDataReporter<T> {

    abstract T generateLoadData();

    protected double percentChange(final double oldValue, final double newValue) {
        if (oldValue == 0) {
            if (newValue == 0) {
                // Avoid NaN
                return 0;
            }
            return Double.POSITIVE_INFINITY;
        }
        return 100 * Math.abs((oldValue - newValue) / oldValue);
    }

}
