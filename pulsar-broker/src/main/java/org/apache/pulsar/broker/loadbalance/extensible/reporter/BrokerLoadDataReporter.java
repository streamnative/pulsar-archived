package org.apache.pulsar.broker.loadbalance.extensible.reporter;

import io.netty.util.concurrent.DefaultThreadFactory;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.SystemUtils;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.loadbalance.BrokerHostUsage;
import org.apache.pulsar.broker.loadbalance.impl.GenericBrokerHostUsageImpl;
import org.apache.pulsar.broker.loadbalance.impl.LinuxBrokerHostUsageImpl;
import org.apache.pulsar.broker.loadbalance.impl.LoadManagerShared;
import org.apache.pulsar.broker.loadbalance.extensible.data.BrokerLoadData;
import org.apache.pulsar.broker.loadbalance.extensible.data.LoadDataStore;
import org.apache.pulsar.broker.loadbalance.extensible.data.LoadDataStoreException;
import org.apache.pulsar.policies.data.loadbalancer.NamespaceBundleStats;
import org.apache.pulsar.policies.data.loadbalancer.SystemResourceUsage;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/**
 * The broker load data reporter.
 */
@Slf4j
public class BrokerLoadDataReporter extends AbstractLoadDataReporter<BrokerLoadData> {

    private final PulsarService pulsar;

    private final ServiceConfiguration conf;

    private final LoadDataStore<BrokerLoadData> brokerLoadDataStore;

    private final BrokerHostUsage brokerHostUsage;

    private final String lookupServiceAddress;

    private final ScheduledExecutorService executor;

    private final BrokerLoadData localData;

    private final BrokerLoadData lastData;

    private volatile ScheduledFuture<?> scheduledFuture;

    public BrokerLoadDataReporter(LoadDataStore<BrokerLoadData> brokerLoadDataStore,
                                  PulsarService pulsar,
                                  String lookupServiceAddress) {
        this.brokerLoadDataStore = brokerLoadDataStore;
        this.lookupServiceAddress = lookupServiceAddress;
        this.pulsar = pulsar;
        this.conf = this.pulsar.getConfiguration();
        if (SystemUtils.IS_OS_LINUX) {
            brokerHostUsage = new LinuxBrokerHostUsageImpl(pulsar);
        } else {
            brokerHostUsage = new GenericBrokerHostUsageImpl(pulsar);
        }
        this.executor = Executors
                .newSingleThreadScheduledExecutor(new DefaultThreadFactory("broker-load-data-reporter"));
        this.localData = new BrokerLoadData();
        this.lastData = new BrokerLoadData();

    }

    @Override
    public BrokerLoadData generateLoadData() {
        final SystemResourceUsage systemResourceUsage = LoadManagerShared.getSystemResourceUsage(brokerHostUsage);
        localData.update(systemResourceUsage, getBundleStats());
        localData.setLastUpdate(System.currentTimeMillis());
        return this.localData;
    }

    private Map<String, NamespaceBundleStats> getBundleStats() {
        return pulsar.getBrokerService().getBundleStats();
    }

    @Override
    public void start() {
        this.scheduledFuture =
                this.executor.scheduleAtFixedRate(this::schedulerFlush, 0, 10, TimeUnit.SECONDS);

    }

    @Override
    public void flush() {
        try {
            this.brokerLoadDataStore.push(this.lookupServiceAddress, this.generateLoadData());
            this.localData.cleanDeltas();
        } catch (LoadDataStoreException ex) {
            log.error("Flush the broker load data failed.", ex);
        }
    }

    private void schedulerFlush() {
        if (needBrokerDataUpdate()) {
            this.flush();
        }
    }

    private boolean needBrokerDataUpdate() {
        int loadBalancerReportUpdateMaxIntervalMinutes = conf.getLoadBalancerReportUpdateMaxIntervalMinutes();
        int loadBalancerReportUpdateThresholdPercentage = conf.getLoadBalancerReportUpdateThresholdPercentage();
        final long updateMaxIntervalMillis = TimeUnit.MINUTES
                .toMillis(loadBalancerReportUpdateMaxIntervalMinutes);
        long timeSinceLastReportWrittenToStore = System.currentTimeMillis() - localData.getLastUpdate();
        if (timeSinceLastReportWrittenToStore > updateMaxIntervalMillis) {
            log.info("Writing local data to metadata store because time since last"
                            + " update exceeded threshold of {} minutes",
                    loadBalancerReportUpdateMaxIntervalMinutes);
            // Always update after surpassing the maximum interval.
            return true;
        }
        final double maxChange = Math
                .max(100.0 * (Math.abs(lastData.getMaxResourceUsage() - localData.getMaxResourceUsage())),
                        Math.max(percentChange(lastData.getMsgRateIn() + lastData.getMsgRateOut(),
                                        localData.getMsgRateIn() + localData.getMsgRateOut()),
                            Math.max(
                                percentChange(lastData.getMsgThroughputIn() + lastData.getMsgThroughputOut(),
                                        localData.getMsgThroughputIn() + localData.getMsgThroughputOut()),
                                percentChange(lastData.getNumBundles(), localData.getNumBundles()))));
        if (maxChange > loadBalancerReportUpdateThresholdPercentage) {
            log.info("Writing local data to metadata store because maximum change {}% exceeded threshold {}%; "
                            + "time since last report written is {} seconds", maxChange,
                    loadBalancerReportUpdateThresholdPercentage,
                    timeSinceLastReportWrittenToStore / 1000.0);
            return true;
        }
        return false;
    }

    @Override
    public void close() throws Exception {
        scheduledFuture.cancel(false);
    }
}
