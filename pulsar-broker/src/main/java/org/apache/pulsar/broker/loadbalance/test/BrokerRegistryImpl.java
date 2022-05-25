package org.apache.pulsar.broker.loadbalance.test;

import com.google.common.collect.Sets;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.broker.PulsarServerException;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.common.util.FutureUtil;
import org.apache.pulsar.metadata.api.MetadataStoreException;
import org.apache.pulsar.metadata.api.Notification;
import org.apache.pulsar.metadata.api.coordination.LockManager;
import org.apache.pulsar.metadata.api.coordination.ResourceLock;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

@Slf4j
public class BrokerRegistryImpl implements BrokerRegistry {

    private final String LOOKUP_DATA_PATH = "/loadbalance/brokers";

    private final PulsarService pulsar;

    private final ServiceConfiguration conf;

    private final BrokerLookupData brokerLookupData;

    private final LockManager<BrokerLookupData> brokerLookupDataLockManager;

    private final String brokerZNodePath;

    private final String lookupServiceAddress;

    private final Set<String> availableBrokers;

    private final AtomicBoolean registered;

    private volatile ResourceLock<BrokerLookupData> brokerLookupDataLock;

    public BrokerRegistryImpl(PulsarService pulsar) {
        this.pulsar = pulsar;
        this.conf = pulsar.getConfiguration();
        this.brokerLookupDataLockManager = pulsar.getCoordinationService().getLockManager(BrokerLookupData.class);
        this.availableBrokers = new ConcurrentSkipListSet<>();

        this.registered = new AtomicBoolean(false);
        this.brokerLookupData = new BrokerLookupData(
                pulsar.getSafeWebServiceAddress(),
                pulsar.getWebServiceAddressTls(),
                pulsar.getBrokerServiceUrl(),
                pulsar.getBrokerServiceUrlTls(),
                pulsar.getAdvertisedListeners());
        // At this point, the ports will be updated with the real port number that the server was assigned
        Map<String, String> protocolData = pulsar.getProtocolDataToAdvertise();
        this.brokerLookupData.setProtocols(protocolData);
        // configure broker-topic mode
        this.brokerLookupData.setPersistentTopicsEnabled(pulsar.getConfiguration().isEnablePersistentTopics());
        this.brokerLookupData.setNonPersistentTopicsEnabled(pulsar.getConfiguration().isEnableNonPersistentTopics());
        this.brokerLookupData.setBrokerVersion(pulsar.getBrokerVersion());

        this.lookupServiceAddress = pulsar.getAdvertisedAddress() + ":"
                + conf.getWebServicePort().orElseGet(() -> conf.getWebServicePortTls().get());
        this.brokerZNodePath = LOOKUP_DATA_PATH + "/" + lookupServiceAddress;
    }

    @Override
    public void start() {
        pulsar.getLocalMetadataStore().registerListener(this::handleDataNotification);
    }

    @Override
    public void register() {
        if (registered.compareAndSet(false, true)) {
            this.brokerLookupDataLock
                    = brokerLookupDataLockManager.acquireLock(brokerZNodePath, brokerLookupData).join();
        }
    }

    @Override
    public void unregister() throws PulsarServerException {
        if (registered.compareAndSet(true, false)) {
            try {
                brokerLookupDataLock.release().join();
            } catch (CompletionException e) {
                if (e.getCause() instanceof MetadataStoreException.NotFoundException) {
                    throw new PulsarServerException.NotFoundException(MetadataStoreException.unwrap(e));
                } else {
                    throw new PulsarServerException(MetadataStoreException.unwrap(e));
                }
            }
        }
    }

    @Override
    public String getLookupServiceAddress() {
        return this.lookupServiceAddress;
    }

    @Override
    public Set<String> getAvailableBrokers() {
        try {
            return new HashSet<>(
                    this.brokerLookupDataLockManager.listLocks(LOOKUP_DATA_PATH)
                    .get(conf.getMetadataStoreOperationTimeoutSeconds(), TimeUnit.SECONDS));
        } catch (Exception e) {
            log.warn("Error when trying to get active brokers", e);
            return this.availableBrokers;
        }
    }

    @Override
    public CompletableFuture<Set<String>> getAvailableBrokersAsync() {
        CompletableFuture<Set<String>> future = new CompletableFuture<>();
        brokerLookupDataLockManager.listLocks(LOOKUP_DATA_PATH)
                .whenComplete((listLocks, ex) -> {
                    if (ex != null){
                        Throwable realCause = FutureUtil.unwrapCompletionException(ex);
                        log.warn("Error when trying to get active brokers", realCause);
                        future.complete(availableBrokers);
                    } else {
                        future.complete(Sets.newHashSet(listLocks));
                    }
                });
        return future;
    }

    @Override
    public BrokerLookupData lookup(String broker) {
        String key = String.format("%s/%s", LOOKUP_DATA_PATH, broker);
        try {
            return brokerLookupDataLockManager.readLock(key).join().orElse(null);
        } catch (Exception e) {
            log.warn("Failed to get local-broker data for {}", broker, e);
            return null;
        }
    }

    @Override
    public void close() throws Exception {
        this.unregister();
    }

    private void handleDataNotification(Notification t) {
        if (t.getPath().startsWith(LOOKUP_DATA_PATH)) {
            this.brokerLookupDataLockManager.listLocks(LOOKUP_DATA_PATH)
                    .thenAccept(brokers -> {
                        this.availableBrokers.clear();
                        this.availableBrokers.addAll(brokers);
                    });
        }
    }
}
