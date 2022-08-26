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

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.commons.lang3.mutable.MutableInt;
import org.apache.pulsar.broker.PulsarServerException;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.loadbalance.extensible.data.Ownership;
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

    private final Cache<String, Ownership> ownershipCache;
    private final Producer<BundleStateData> producer;

    private final PulsarService pulsar;

    private final ConcurrentOpenHashMap<String, CompletableFuture<Optional<String>>>
            assigningBundles = ConcurrentOpenHashMap.<String,
                    CompletableFuture<Optional<String>>>newBuilder()
            .build();

    public static final String TOPIC =
            TopicDomain.persistent.value()
                    + "://"
                    + NamespaceName.SYSTEM_NAMESPACE
                    + "/bundle-state-channel";

    public BundleStateChannel(PulsarService pulsar)
            throws PulsarServerException {
        this.pulsar = pulsar;


        try {
            val schema = JSONSchema.of(BundleStateData.class);
            tv = pulsar.getClient().newTableViewBuilder(schema)
                    .topic(TOPIC)
                    .create();
            producer = pulsar.getClient().newProducer(schema)
                    .topic(TOPIC)
                    .create();
            ownershipCache = Caffeine.newBuilder()
                    .expireAfterWrite(30, TimeUnit.SECONDS)
                    .build();
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
            case Assigned -> handleAssigned(bundle, data);
            case Assigning -> handleAssigning(bundle, data);
            case Unassigned -> handleUnassigned(bundle, data);
            default -> {
                throw new IllegalStateException("Failed to handle bundle state data:" + data.toString());
            }
        }
    }

    private void handleAssigned(String bundle, BundleStateData data) {
        ownershipCache.invalidate(bundle);
        val assigning = assigningBundles.remove(bundle);
        if (assigning != null) {
            assigning.complete(Optional.of(data.getBroker()));
        }

        if (isTargetBroker(data.getSourceBroker())) {
            // TODO: when close, pass message to clients to connect to the new broker
            disableOwnership(bundle);
            closeBundle(bundle);
        }

        //TODO: remove log
        log.info("handled-Assigned:{},{}", bundle, data);
    }


    private void handleAssigning(String bundle, BundleStateData data) {

        if (isTargetBroker(data.getSourceBroker())) {
            disableOwnership(bundle);
            return;
        }

        if (isTargetBroker(data.getBroker())) {
            BundleStateData next = new BundleStateData(
                    BundleState.Assigned, data.getBroker(), data.getSourceBroker());
            ownershipCache.put(bundle, new Ownership(true, next.getBroker()));
            pubAsync(bundle, next);
            //TODO: remove log
            log.info("broker {} handled-Assigning:{},{}", pulsar.getBrokerServiceUrl(), bundle, data);
        }
    }


    private void handleUnassigned(String bundle, BundleStateData data) {
        if (isTargetBroker(data.getBroker())) {
            disableOwnership(bundle);
            closeBundle(bundle)
                    .thenAccept(x -> tombstoneAsync(bundle)); // TODO: do we need to tombstoneAsync unload bundle?
            //TODO: remove log
            log.info("handled-UnAssigned:{},{}", bundle, data);
        }
    }

    private void handleTombstone(String bundle) {
        ownershipCache.invalidate(bundle);
        assigningBundles.remove(bundle).complete(Optional.empty());
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
        return broker.startsWith(pulsar.getAdvertisedAddress());
    }

    private NamespaceBundle getNamespaceBundle(String bundle) {
        final String namespaceName = LoadManagerShared.getNamespaceNameFromBundleName(bundle);
        final String bundleRange = LoadManagerShared.getBundleRangeFromBundleName(bundle);
        return pulsar.getNamespaceService().getNamespaceBundleFactory().getBundle(namespaceName, bundleRange);
    }


    private CompletableFuture<Optional<String>> waitForAssignment(String bundle) {
        CompletableFuture future = new CompletableFuture<>()
                .orTimeout(30, TimeUnit.SECONDS);
        return assigningBundles
                .computeIfAbsent(bundle, k -> future);
    }

    private void disableOwnership(String bundle) {
        Ownership ownership = ownershipCache.getIfPresent(bundle);
        if (ownership.isActive()) {
            ownership.setActive(false);
        } else {
            BundleStateData data = tv.get(bundle);
            if (data == null) {
                throw new IllegalStateException("no bundle state in the channel while disabling ownership");
            }
            ownershipCache.put(bundle, new Ownership(false, data.getBroker()));
        }
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

    private static CompletableFuture<Optional<String>> completeOwnership(String broker){
        return CompletableFuture.completedFuture(Optional.of(broker));
    }

    public CompletableFuture<Optional<String>> getOwner(String bundle) {

        Ownership ownership = ownershipCache.getIfPresent(bundle);
        if (ownership != null) {
            if (ownership.isActive()) {
                return completeOwnership(ownership.getBroker());
            } else {
                return waitForAssignment(bundle);
            }
        }
        BundleStateData data = tv.get(bundle);
        if (data == null) {
            return null;
        } else if (data.getState() == BundleState.Assigned) {
            return completeOwnership(data.getBroker());
        } else if (data.getState() == BundleState.Assigning) {
            return waitForAssignment(bundle);
        }
        return null;
    }

    public CompletableFuture<Optional<String>> assignBundle(String bundle, String broker) {
        CompletableFuture<Optional<String>> future = waitForAssignment(bundle);
        return pubAsync(bundle, new BundleStateData(BundleState.Assigning, broker))
                .thenCompose(x -> future)
                .exceptionally(e -> {
                    assigningBundles.remove(bundle);
                    future.complete(Optional.empty());
                    return Optional.empty();
                });
    }


    // TODO make it CompletableFuture
    public void unloadBundle(Unload unload) {
        String bundle = unload.getBundle();
        BundleStateData data = tv.get(bundle);
        if (data.getBroker().equals(unload.getSourceBroker())) {
            throw new IllegalStateException("source broker does not match with the current state broker");
        }
        BundleStateData next = unload.getDestBroker().isPresent()
                ? new BundleStateData(BundleState.Assigning, unload.getDestBroker().get(), unload.getSourceBroker())
                : new BundleStateData(BundleState.Unassigned, data.getBroker());
        pubAsync(bundle, next);
    }
}
