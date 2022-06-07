package org.apache.pulsar.broker.loadbalance.extensible.reporter;

import org.apache.pulsar.broker.loadbalance.extensible.data.LoadDataStore;
import org.apache.pulsar.policies.data.loadbalancer.BundleData;

import java.util.Map;

public class BundleLoadDataReporter extends AbstractLoadDataReporter<Map<String, BundleData>> {

    private final LoadDataStore<BundleData> loadDataStore;

    public BundleLoadDataReporter(LoadDataStore<BundleData> loadDataStore) {
        this.loadDataStore = loadDataStore;
    }

    @Override
    public Map<String, BundleData> generateLoadData() {
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
