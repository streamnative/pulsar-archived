package org.apache.pulsar.broker.loadbalance.test.reporter;

import org.apache.pulsar.broker.loadbalance.test.data.BundleLoadData;
import org.apache.pulsar.broker.loadbalance.test.data.LoadDataStore;

import java.util.Map;

public class BundleLoadDataReporter extends AbstractLoadDataReporter<BundleLoadData> {

    public BundleLoadDataReporter() {
        super();
    }

    @Override
    LoadDataStore<BundleLoadData> getLoadDataStore() {
        return null;
    }

    @Override
    Map<String, BundleLoadData> generateLoadData() {
        return null;
    }

    @Override
    public void start() {

    }

    @Override
    public void close() throws Exception {

    }
}
