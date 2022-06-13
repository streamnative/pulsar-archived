package org.apache.pulsar.broker.loadbalance.extensible.reporter;

import org.apache.pulsar.broker.loadbalance.extensible.data.BrokerLoadData;
import org.apache.pulsar.policies.data.loadbalancer.BundleData;
import org.apache.pulsar.policies.data.loadbalancer.TimeAverageBrokerData;

import java.util.Map;
import java.util.concurrent.CompletableFuture;

public interface LoadDataReport {
    void start();

    void close();

    CompletableFuture<Void> reportBrokerLoadDataAsync();

    CompletableFuture<Void> reportBundleLoadDataAsync();

    CompletableFuture<Void> reportTimeAverageBrokerDataAsync();

    BrokerLoadData generateBrokerLoadData();

    Map<String, BundleData> generateBundleData();

    TimeAverageBrokerData generateTimeAverageBrokerData();
}
