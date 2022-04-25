package org.apache.pulsar.broker.loadbalance.test.reporter;

import org.apache.pulsar.broker.loadbalance.test.data.LoadDataStore;
import org.apache.pulsar.broker.loadbalance.test.data.TimeAverageBrokerLoadData;

import java.util.Map;

public class TimeAverageBrokerLoadDataReporter extends AbstractLoadDataReporter<TimeAverageBrokerLoadData> {

    public TimeAverageBrokerLoadDataReporter() {
        super();
    }

    @Override
    public LoadDataStore<TimeAverageBrokerLoadData> getLoadDataStore() {
        return null;
    }

    @Override
    public Map<String, TimeAverageBrokerLoadData> generateLoadData() {
        return null;
    }

    @Override
    public void start() {

    }

    @Override
    public void close() throws Exception {

    }
}
