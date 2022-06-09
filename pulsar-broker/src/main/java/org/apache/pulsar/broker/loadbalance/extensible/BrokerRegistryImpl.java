/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pulsar.broker.loadbalance.extensible;

import com.google.common.collect.Lists;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiConsumer;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.broker.PulsarServerException;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.common.util.FutureUtil;
import org.apache.pulsar.metadata.api.MetadataStoreException;
import org.apache.pulsar.metadata.api.Notification;
import org.apache.pulsar.metadata.api.coordination.LockManager;
import org.apache.pulsar.metadata.api.coordination.ResourceLock;

/**
 * The broker registry impl, base on the LockManager.
 */
@Slf4j
public class BrokerRegistryImpl implements BrokerRegistry {

    private static final String LOOKUP_DATA_PATH = "/loadbalance/brokers";

    private final PulsarService pulsar;

    private final ServiceConfiguration conf;

    private final BrokerLookupData brokerLookupData;

    private final LockManager<BrokerLookupData> brokerLookupDataLockManager;

    private final String brokerZNodePath;

    private final String lookupServiceAddress;

    private final Map<String, BrokerLookupData> brokerLookupDataMap;

    private final ScheduledExecutorService scheduler;

    private final AtomicBoolean registered;

    private volatile ResourceLock<BrokerLookupData> brokerLookupDataLock;

    public BrokerRegistryImpl(PulsarService pulsar) {
        this.pulsar = pulsar;
        this.conf = pulsar.getConfiguration();
        this.brokerLookupDataLockManager = pulsar.getCoordinationService().getLockManager(BrokerLookupData.class);
        this.scheduler = pulsar.getLoadManagerExecutor();
        this.brokerLookupDataMap = new ConcurrentHashMap<>();

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
            this.brokerLookupDataLock =
                    brokerLookupDataLockManager.acquireLock(brokerZNodePath, brokerLookupData).join();
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
    public List<String> getAvailableBrokers() {
        try {
            return this.brokerLookupDataLockManager.listLocks(LOOKUP_DATA_PATH)
                    .get(conf.getMetadataStoreOperationTimeoutSeconds(), TimeUnit.SECONDS);
        } catch (Exception e) {
            log.warn("Error when trying to get active brokers", e);
            return Lists.newArrayList(this.brokerLookupDataMap.keySet());
        }
    }

    @Override
    public CompletableFuture<List<String>> getAvailableBrokersAsync() {
        CompletableFuture<List<String>> future = new CompletableFuture<>();
        brokerLookupDataLockManager.listLocks(LOOKUP_DATA_PATH)
                .whenComplete((listLocks, ex) -> {
                    if (ex != null){
                        Throwable realCause = FutureUtil.unwrapCompletionException(ex);
                        log.warn("Error when trying to get active brokers", realCause);
                        future.complete(Lists.newArrayList(this.brokerLookupDataMap.keySet()));
                    } else {
                        future.complete(Lists.newArrayList(listLocks));
                    }
                });
        return future;
    }

    @Override
    public Optional<BrokerLookupData> lookup(String broker) {
        String key = String.format("%s/%s", LOOKUP_DATA_PATH, broker);
        try {
            return brokerLookupDataLockManager.readLock(key).join();
        } catch (Exception e) {
            log.warn("Failed to get local-broker data for {}", broker, e);
            return Optional.empty();
        }
    }

    @Override
    public void forEach(BiConsumer<String, BrokerLookupData> action) {
        this.brokerLookupDataMap.forEach(action);
    }

    @Override
    public void close() throws Exception {
        this.unregister();
    }

    private void handleDataNotification(Notification t) {
        if (t.getPath().startsWith(LOOKUP_DATA_PATH)) {
            try {
                this.scheduler.submit(this::updateAllBrokerLookupData);
            } catch (RejectedExecutionException e) {
                // Executor is shutting down
            }
        }
    }

    private void updateAllBrokerLookupData() {
        final List<String> activeBrokers = getAvailableBrokers();
        for (String broker : activeBrokers) {
            try {
                String key = String.format("%s/%s", LOOKUP_DATA_PATH, broker);
                Optional<BrokerLookupData> lookupData = brokerLookupDataLockManager.readLock(key).get();
                if (lookupData.isEmpty()) {
                    brokerLookupDataMap.remove(broker);
                    log.info("[{}] Broker lookup data is not present", broker);
                    continue;
                }

                if (brokerLookupDataMap.containsKey(broker)) {
                    // Replace previous local broker lookup data.
                    brokerLookupDataMap.put(broker, lookupData.get());
                } else {
                    // Initialize lookup data object for previously unseen brokers.
                    brokerLookupDataMap.put(broker, lookupData.get());
                }
            } catch (Exception e) {
                log.warn("Error reading broker data from cache for broker - [{}], [{}]", broker, e.getMessage());
            }
        }
        // Remove obsolete brokers.
        for (final String broker : brokerLookupDataMap.keySet()) {
            if (!activeBrokers.contains(broker)) {
                brokerLookupDataMap.remove(broker);
            }
        }
    }
}
