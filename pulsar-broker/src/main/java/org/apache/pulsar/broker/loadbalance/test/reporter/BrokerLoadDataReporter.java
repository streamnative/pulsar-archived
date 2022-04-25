package org.apache.pulsar.broker.loadbalance.test.reporter;

import org.apache.pulsar.broker.loadbalance.test.data.BrokerLoadData;
import org.apache.pulsar.broker.loadbalance.test.data.LoadDataStore;

import java.util.Map;

public class BrokerLoadDataReporter extends AbstractLoadDataReporter<BrokerLoadData> {

    public BrokerLoadDataReporter() {
        super();
    }

    @Override
    public LoadDataStore<BrokerLoadData> getLoadDataStore() {
        return null;
    }

    @Override
    public Map<String, BrokerLoadData> generateLoadData() {
        return null;
    }

    @Override
    public void start() {

    }

    @Override
    public void close() throws Exception {

    }
}
