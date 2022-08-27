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
package org.apache.pulsar.broker.loadbalance.extensible.channel;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.commons.lang3.mutable.MutableInt;
import org.apache.pulsar.broker.PulsarServerException;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.loadbalance.extensible.data.Unload;
import org.apache.pulsar.broker.loadbalance.impl.LoadManagerShared;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.TableView;
import org.apache.pulsar.client.impl.schema.JSONSchema;
import org.apache.pulsar.common.naming.NamespaceBundle;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.naming.TopicDomain;
import org.apache.pulsar.common.util.collections.ConcurrentOpenHashMap;

@Slf4j
public class BundleStateChannel {

    private final TableView<BundleStateData> tv;

    private final Producer<BundleStateData> producer;

    private final PulsarService pulsar;

    private final ConcurrentOpenHashMap<String, CompletableFuture<Optional<String>>>
            lookupRequests = ConcurrentOpenHashMap.<String,
                    CompletableFuture<Optional<String>>>newBuilder()
            .build();

    public static final String TOPIC =
            TopicDomain.persistent.value()
                    + "://"
                    + NamespaceName.SYSTEM_NAMESPACE
                    + "/bundle-state-channel";

    private String lookupServiceAddress;

    public BundleStateChannel(PulsarService pulsar)
            throws PulsarServerException {
        this.pulsar = pulsar;
        var conf = pulsar.getConfiguration();
        this.lookupServiceAddress = pulsar.getAdvertisedAddress() + ":"
                + (conf.getWebServicePort().isPresent() ? conf.getWebServicePort().get()
                : conf.getWebServicePortTls().get());

        try {
            val schema = JSONSchema.of(BundleStateData.class);
            tv = pulsar.getClient().newTableViewBuilder(schema)
                    .topic(TOPIC)
                    .create();
            producer = pulsar.getClient().newProducer(schema)
                    .topic(TOPIC)
                    .create();
        } catch (Exception e) {
            String msg = "Failed to init bundle state channel";
            e.printStackTrace();
            log.error(msg, e);
            throw new PulsarServerException(msg, e);
        }

        tv.forEachAndListen((key, value) -> handle(key, value));
    }

    private void handle(String bundle, BundleStateData data) {
        if (data == null) {
            handleTombstone(bundle);
            return;
        }
        // TODO : Add state validation
        switch (data.getState()) {
            case Assigned -> handleAssignedState(bundle, data);
            case Assigning -> handleAssigningState(bundle, data);
            case Unloading -> handleUnloadingState(bundle, data);
            default -> throw new IllegalStateException("Failed to handle bundle state data:" + data.toString());
        }
    }

    private void handleAssignedState(String bundle, BundleStateData data) {
        if (isTargetBroker(data.getSourceBroker())) {

            // TODO: think if we need to temporarily disable the state util it finishes topic close.
            // => probably not required.
            //data.setState(BundleState.Disabled);
            log.info("{} disabled bundle ownership:{},{}", lookupServiceAddress, bundle, data);
            // TODO: when close, pass message to clients to connect to the new broker
            closeBundle(bundle);
            log.info("{} closed bundle topics:{},{}", lookupServiceAddress, bundle, data);
            //data.setState(BundleState.Assigned);
        }
        val lookupRequest = lookupRequests.remove(bundle);
        if (lookupRequest != null) {
            lookupRequest.complete(Optional.of(data.getBroker()));
            log.info("{} returned deferred lookups:{},{}", lookupServiceAddress, bundle, data);
        }


    }


    private void handleAssigningState(String bundle, BundleStateData data) {


        if (isTargetBroker(data.getSourceBroker())) {
            log.info("{} updated bundle state:{},{}", lookupServiceAddress, bundle, data);
            return;
        }

        if (isTargetBroker(data.getBroker())) {
            // preset
            data.setState(BundleState.Assigned);
            pubAsync(bundle, data);
            //TODO: remove log
            log.info("{} published :{},{}",
                    lookupServiceAddress, pulsar.getBrokerServiceUrl(), bundle, data);

        }

    }


    private void handleUnloadingState(String bundle, BundleStateData data) {
        if (isTargetBroker(data.getBroker())) {
            closeBundle(bundle)
                    .thenAccept(x -> tombstoneAsync(bundle));
            log.info("{} closed topics and published tombstone:{},{}", lookupServiceAddress, bundle, data);
        }
    }

    private void handleTombstone(String bundle) {
        var request = lookupRequests.remove(bundle);
        if (request != null) {
            request.complete(Optional.empty());
            log.info("{} returned deferred lookups:{}", lookupServiceAddress, bundle);
        }

    }


    private CompletableFuture<MessageId> pubAsync(String bundle, BundleStateData data) {
        return producer.newMessage()
                .key(bundle)
                .value(data)
                .sendAsync();
    }

    private CompletableFuture<MessageId> tombstoneAsync(String bundle) {
        return producer.newMessage()
                .key(bundle)
                .sendAsync();
    }

    private boolean isTargetBroker(String broker) {
        if (broker == null) {
            return false;
        }
        // TODO: remove broker port from the input broker
        return broker.equals(lookupServiceAddress);
    }

    private NamespaceBundle getNamespaceBundle(String bundle) {
        final String namespaceName = LoadManagerShared.getNamespaceNameFromBundleName(bundle);
        final String bundleRange = LoadManagerShared.getBundleRangeFromBundleName(bundle);
        return pulsar.getNamespaceService().getNamespaceBundleFactory().getBundle(namespaceName, bundleRange);
    }


    private CompletableFuture<Optional<String>> deferLookUpRequest(String bundle) {
        CompletableFuture future = new CompletableFuture<>()
                .orTimeout(30, TimeUnit.SECONDS);
        return lookupRequests
                .computeIfAbsent(bundle, k -> future);
    }

    private CompletableFuture<Integer> closeBundle(String bundleName) {
        long unloadBundleStartTime = System.nanoTime();
        MutableInt unloadedTopics = new MutableInt();
        NamespaceBundle bundle = getNamespaceBundle(bundleName);
        return pulsar.getBrokerService().unloadServiceUnit(
                        bundle,
                        false,
                        pulsar.getConfig().getNamespaceBundleUnloadingTimeoutMs(),
                        TimeUnit.MILLISECONDS)
                .handle((numUnloadedTopics, ex) -> {
                    if (ex != null) {
                        // ignore topic-close failure to unload bundle
                        log.error("Failed to close topics under namespace {}", bundle.toString(), ex);
                    } else {
                        unloadedTopics.setValue(numUnloadedTopics);
                    }
                    // clean up topics that failed to unload from the broker ownership cache
                    pulsar.getBrokerService().cleanUnloadedTopicFromCache(bundle);
                    return numUnloadedTopics;
                })
                .whenComplete((ignored, ex) -> {
                    double unloadBundleTime = TimeUnit.NANOSECONDS
                            .toMillis((System.nanoTime() - unloadBundleStartTime));
                    log.info("Unloading {} namespace-bundle with {} topics completed in {} ms", bundle,
                            unloadedTopics, unloadBundleTime, ex);
                });
    }

    public CompletableFuture<Optional<String>> getOwner(String bundle) {

        BundleStateData data = tv.get(bundle);
        if (data == null) {
            return null;
        }
        switch (data.getState()) {
            case Assigned -> {
                return CompletableFuture.completedFuture(Optional.of(data.getBroker()));
            }
            case Assigning, Unloading -> {
                return deferLookUpRequest(bundle);
            }
            default -> {
                return null;
            }
        }
    }

    public CompletableFuture<Optional<String>> assignBundle(String bundle, String broker) {
        CompletableFuture<Optional<String>> lookupRequest = deferLookUpRequest(bundle);
        return pubAsync(bundle, new BundleStateData(BundleState.Assigning, broker))
                .thenCompose(x -> lookupRequest)
                .exceptionally(e -> {
                    lookupRequests.remove(bundle);
                    lookupRequest.complete(Optional.empty());
                    return Optional.empty();
                });
    }


    // TODO make it CompletableFuture
    public void unloadBundle(Unload unload) {
        String bundle = unload.getBundle();
        BundleStateData data = tv.get(bundle);
        BundleStateData next = unload.getDestBroker().isPresent()
                ? new BundleStateData(BundleState.Assigning, unload.getDestBroker().get(), unload.getSourceBroker())
                : new BundleStateData(BundleState.Unloading, data.getBroker());
        pubAsync(bundle, next);
    }
}
