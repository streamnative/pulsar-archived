package org.apache.pulsar.broker.loadbalance.extensible.reporter;

import org.apache.pulsar.broker.loadbalance.extensible.data.LoadDataStore;
import org.apache.pulsar.broker.loadbalance.extensible.data.TimeAverageBrokerLoadData;

import java.util.Map;

public class TimeAverageBrokerLoadDataReporter extends AbstractLoadDataReporter<TimeAverageBrokerLoadData> {

    private final LoadDataStore<TimeAverageBrokerLoadData> loadDataStore;

    public TimeAverageBrokerLoadDataReporter(LoadDataStore<TimeAverageBrokerLoadData> loadDataStore) {
        this.loadDataStore = loadDataStore;
    }


    @Override
    public TimeAverageBrokerLoadData generateLoadData() {
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
