package org.apache.pulsar.broker.loadbalance.extensible.reporter;

import org.apache.pulsar.broker.loadbalance.extensible.data.LoadDataStore;
import org.apache.pulsar.policies.data.loadbalancer.TimeAverageBrokerData;

public class TimeAverageBrokerLoadDataReporter extends AbstractLoadDataReporter<TimeAverageBrokerData> {

    private final LoadDataStore<TimeAverageBrokerData> loadDataStore;

    public TimeAverageBrokerLoadDataReporter(LoadDataStore<TimeAverageBrokerData> loadDataStore) {
        this.loadDataStore = loadDataStore;
    }


    @Override
    public TimeAverageBrokerData generateLoadData() {
        return null;
    }

    @Override
    public void start() {

    }

    @Override
    public void flush() {

    }

    @Override
    public void close() throws Exception {

    }
}
