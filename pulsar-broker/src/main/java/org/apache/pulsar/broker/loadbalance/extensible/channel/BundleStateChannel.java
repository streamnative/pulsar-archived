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

import static org.apache.pulsar.broker.loadbalance.extensible.channel.BundleState.Assigning;
import static org.apache.pulsar.broker.loadbalance.extensible.channel.BundleState.inFlightStates;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.commons.lang3.mutable.MutableInt;
import org.apache.pulsar.broker.PulsarServerException;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.loadbalance.LeaderBroker;
import org.apache.pulsar.broker.loadbalance.LeaderElectionService;
import org.apache.pulsar.broker.loadbalance.extensible.BrokerRegistry;
import org.apache.pulsar.broker.loadbalance.extensible.ExtensibleLoadManagerWrapper;
import org.apache.pulsar.broker.loadbalance.extensible.data.Split;
import org.apache.pulsar.broker.loadbalance.extensible.data.Unload;
import org.apache.pulsar.broker.loadbalance.impl.LoadManagerShared;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.TableView;
import org.apache.pulsar.client.impl.schema.JSONSchema;
import org.apache.pulsar.common.naming.NamespaceBundle;
import org.apache.pulsar.common.naming.NamespaceBundleSplitAlgorithm;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.naming.ServiceUnitId;
import org.apache.pulsar.common.naming.TopicDomain;
import org.apache.pulsar.common.topics.TopicCompactionStrategy;
import org.apache.pulsar.common.util.collections.ConcurrentOpenHashMap;
import org.apache.pulsar.metadata.api.coordination.LeaderElectionState;

@Slf4j
public class BundleStateChannel {

    private TableView<BundleStateData> tv;

    private Producer<BundleStateData> producer;

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

    public static final JSONSchema<BundleStateData> SCHEMA = JSONSchema.of(BundleStateData.class);

    public static final TopicCompactionStrategy STRATEGY =
            TopicCompactionStrategy.load(BundleStateCompactionStrategy.class.getName());

    public static final long COMPACTION_THRESHOLD = 5 * 1024 * 1024; // 5mb

    public static final long MAX_IN_FLIGHT_STATE_WAITING_TIME_IN_MILLIS = 30 * 1000; // 30sec

    private String lookupServiceAddress;

    private LeaderElectionService leaderElectionService;

    private BrokerRegistry brokerRegistry;

    private boolean leaderCleanBundleOwnerships;

    public BundleStateChannel(PulsarService pulsar)
            throws PulsarServerException {
        this.pulsar = pulsar;
        var conf = pulsar.getConfiguration();
        this.lookupServiceAddress = pulsar.getAdvertisedAddress() + ":"
                + (conf.getWebServicePort().isPresent() ? conf.getWebServicePort().get()
                : conf.getWebServicePortTls().get());
        this.leaderCleanBundleOwnerships = false;
    }

    public synchronized void start() throws PulsarServerException {

        try {
            if (leaderElectionService != null) {
                log.info("Closing the bundle state leader election service.");
                leaderElectionService.close();
            }
            this.leaderElectionService = new LeaderElectionService(
                    pulsar.getCoordinationService(), pulsar.getSafeWebServiceAddress(),
                    state -> {
                        if (state == LeaderElectionState.Leading) {
                            log.info("This broker was elected as bundleStateChanel leader");
                        } else {
                            if (leaderElectionService != null) {
                                log.info("This broker is a bundleStateChanel follower. "
                                                + "Current bundleStateChanel leader is {}",
                                        leaderElectionService.getCurrentLeader());
                            }
                        }
                    });
            leaderElectionService.start();
            log.info("Successfully started the bundle state leader election service.");
            if (tv != null) {
                log.info("Closing the bundle state tableview.");
                tv.close();
            }
            tv = pulsar.getClient().newTableViewBuilder(SCHEMA)
                    .topic(TOPIC)
                    .loadConf(Map.of(
                            "topicCompactionStrategy", BundleStateCompactionStrategy.class.getName()))
                    .create();
            log.info("Successfully started the bundle state tableview.");
            if (producer != null) {
                log.info("Closing the bundle state producer.");
                producer.close();
            }
            producer = pulsar.getClient().newProducer(SCHEMA)
                    .topic(TOPIC)
                    .create();

            log.info("Successfully started bundle state producer.");
            this.brokerRegistry = ((ExtensibleLoadManagerWrapper) pulsar.getLoadManager().get())
                    .get().getBrokerRegistry();
            this.pulsar.getLoadManagerExecutor()
                    .scheduleWithFixedDelay(() -> {
                                try {
                                    if (leaderElectionService.isLeader()) {
                                        if (leaderCleanBundleOwnerships) {
                                            cleanBundleOwnerships(
                                                    brokerRegistry.getAvailableBrokers());
                                            leaderCleanBundleOwnerships = false;
                                        } else {
                                            cleanOldInFlightBundles();
                                            leaderCleanBundleOwnerships = true;
                                        }
                                    }
                                    log.info("Successfully recovered load manager");
                                } catch (Exception e) {
                                    log.info("Failed to run bundle ownership clean. will retry..", e);
                                }
                            },
                            0, MAX_IN_FLIGHT_STATE_WAITING_TIME_IN_MILLIS * 5, TimeUnit.MILLISECONDS);
            log.info("Successfully started the bundle state recovery executor.");

            tv.listen((key, value) -> handle(key, value));

            log.info("Successfully started the bundle state channel.");

        } catch (Exception e) {
            String msg = "Failed to init bundle state channel.";
            log.error(msg, e);
            throw new PulsarServerException(msg, e);
        }



    }

    public void scheduleBundleStateChannelCompaction() throws PulsarServerException {
        try {
            Long threshold = pulsar.getAdminClient().topicPolicies()
                    .getCompactionThreshold(TOPIC);
            if (threshold == null || threshold == 0) {
                pulsar.getAdminClient().topicPolicies()
                        .setCompactionThreshold(TOPIC, COMPACTION_THRESHOLD);
                log.info("Scheduled compaction on topic:{}, threshold:{} bytes", TOPIC, COMPACTION_THRESHOLD);
            } else {
                log.info("Already set compaction on topic:{}, threshold:{} bytes", TOPIC, COMPACTION_THRESHOLD);
            }
        } catch (PulsarAdminException e) {
            throw new PulsarServerException(e);
        }
    }

    private void handle(String bundle, BundleStateData data) {
        log.info("{} received a handle request for bundle:{}, data:{}", lookupServiceAddress, bundle, data);
        if (data == null) {
            handleTombstone(bundle);
            return;
        }

        // TODO : Add state validation
        switch (data.getState()) {
            case Assigned -> handleAssignedState(bundle, data);
            case Assigning -> handleAssigningState(bundle, data);
            case Unloading -> handleUnloadingState(bundle, data);
            case Splitting -> handleSplittingState(bundle, data);
            default -> throw new IllegalStateException("Failed to handle bundle state data:" + data);
        }
    }

    private void handleAssignedState(String bundle, BundleStateData data) {
        if (isTargetBroker(data.getSourceBroker())) {

            // TODO: think if we need to temporarily disable the state util it finishes topic close.
            // => probably not required.
            log.info("{} disabled bundle ownership:{},{}", lookupServiceAddress, bundle, data);
            // TODO: when close, pass message to clients to connect to the new broker
            closeBundle(bundle);
            log.info("{} closed bundle topics:{},{}", lookupServiceAddress, bundle, data);
        }
        val lookupRequest = lookupRequests.remove(bundle);
        if (lookupRequest != null) {
            lookupRequest.complete(Optional.of(data.getBroker()));
            // TODO: log the lookup delay time.
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
            BundleStateData next =
                    new BundleStateData(bundle, BundleState.Assigned, data.getBroker(), data.getSourceBroker());
            pubAsync(bundle, next);
            //TODO: remove log
            log.info("{} published :{},{},{}",
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

    private void handleSplittingState(String bundle, BundleStateData data) {
        if (isTargetBroker(data.getBroker())) {
            splitBundle(bundle)
                    .thenAccept(x -> tombstoneAsync(bundle));
            log.info("{} split bundle and published tombstone:{},{}", lookupServiceAddress, bundle, data);
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
                .sendAsync()
                /*.thenApply(messageId -> {
                            log.info("Published message for bundle:{}, messageId:{}, data:{}",
                                    bundle, messageId, data);
                            return messageId;
                        }

                 )*/
                .exceptionally(e -> {
                    log.error("Failed to publish message for bundle:{}, data:{}", bundle, data);
                    return null;
                });
    }

    private CompletableFuture<MessageId> tombstoneAsync(String bundle) {

        return producer.newMessage()
                .key(bundle)
                .sendAsync()
                /*
                .thenApply(messageId -> {
                            log.info("Published tombstone for bundle:{}, messageId:{}", bundle, messageId);
                            return messageId;
                        }
                )*/
                .exceptionally(e -> {
                    log.error("Failed to publish tombstone for bundle:{}", bundle);
                    return null;
                });
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
        CompletableFuture future = new CompletableFuture<>();

        future.orTimeout(MAX_IN_FLIGHT_STATE_WAITING_TIME_IN_MILLIS, TimeUnit.MILLISECONDS);

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

    private CompletableFuture<Void> splitBundle(String bundleName) {
        long splitBundleStartTime = System.nanoTime();
        return pulsar.getNamespaceService()
                .splitAndOwnBundle(getNamespaceBundle(bundleName),
                        false,
                        NamespaceBundleSplitAlgorithm.RANGE_EQUALLY_DIVIDE_ALGO,
                        null)
                .whenComplete((__, ex) -> {
                    double splitBundleTime = TimeUnit.NANOSECONDS
                            .toMillis((System.nanoTime() - splitBundleStartTime));
                    log.info("Splitting {} namespace-bundle completed in {} ms",
                            bundleName, splitBundleTime, ex);
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

    public CompletableFuture<Optional<String>> publishAssignment(String bundle, String broker) {
        CompletableFuture<Optional<String>> lookupRequest = deferLookUpRequest(bundle);
        return pubAsync(bundle, new BundleStateData(bundle, Assigning, broker))
                .thenCompose(x -> lookupRequest)
                .exceptionally(e -> {
                    lookupRequests.remove(bundle);
                    lookupRequest.complete(Optional.empty());
                    return Optional.empty();
                });
    }


    // TODO make it CompletableFuture
    public void publishUnload(Unload unload) {
        String bundle = unload.getBundle();
        BundleStateData data = tv.get(bundle);
        BundleStateData next = unload.getDestBroker().isPresent()
                ? new BundleStateData(bundle, Assigning,
                unload.getDestBroker().get(), unload.getSourceBroker())
                : new BundleStateData(bundle, BundleState.Unloading, data.getBroker());
        pubAsync(bundle, next);
    }

    public CompletableFuture<Void> splitBundle(Split split) {
        String bundle = split.getBundle();
        BundleStateData data = tv.get(bundle);
        BundleStateData next = new BundleStateData(bundle, BundleState.Splitting, data.getBroker());
        return pubAsync(bundle, next).thenCompose(__ -> null);
    }

    private Set<BundleStateData> getBunldes(String broker){
        return tv.values().stream()
                .filter(bundle -> broker.equals(bundle.getBroker()))
                .collect(Collectors.toSet());
    }

    private Set<BundleStateData> getBunldes(BundleState state){
        return tv.values().stream()
                .filter(bundle -> state.equals(bundle.getState()))
                .collect(Collectors.toSet());
    }

    private Set<String> getBrokers(){
        return tv.values().stream()
                .map(bundle -> bundle.getBroker())
                .collect(Collectors.toSet());
    }

    public void cleanBundleOwnerships(String broker) {
        if (!leaderElectionService.isLeader()) {
            return;
        }

        Set<BundleStateData> values = getBunldes(broker);
        if (values != null) {
            log.info("Cleaning bundle ownerships for dead broker:{}. "
                            + "Attempting to release bundle bundle count:{}",
                    broker, values == null ? 0 : values.size());
            for (BundleStateData data : values) {
                log.info("Cleaning bundle ownerships for dead broker:{}, bundle:{}",
                        broker, data == null ? "" : data.getBundle());
                tombstoneAsync(data.getBundle());
            }
        }
    }

    public void cleanBundleOwnerships(List<String> brokers) {
        if (!leaderElectionService.isLeader()) {
            return;
        }
        if (brokers == null || brokers.size() == 0) {
            log.warn("no active brokers found. Skipping bundle ownership clean.");
            return;
        }

        log.info("Started bundle ownership cleanup for active broker count:{}", brokers.size());
        int deadBrokerCnt = 0;
        int releasedBundleCnt = 0;
        Set<String> deadBrokers = new HashSet<>();
        Set<String> activeBrokers = new HashSet<>(brokers);
        for (BundleStateData bundleStateData : tv.values()) {
            String broker = bundleStateData.getBroker();
            if (!activeBrokers.contains(broker)) {
                tombstoneAsync(bundleStateData.getBundle());
                releasedBundleCnt++;
                if (deadBrokers.add(bundleStateData.getBroker())) {
                    deadBrokerCnt++;
                }
            }
        }

        log.info("Completed bundle ownership cleanup. Dead broker count:{}, Released bundle count:{}",
                deadBrokerCnt, releasedBundleCnt);
    }

    public void cleanOldInFlightBundles() {
        if (!leaderElectionService.isLeader()) {
            return;
        }
        log.info("Started old in-flight bundle cleanup");
        int releasedBundleCnt = 0;
        long now = System.currentTimeMillis();
        for (BundleStateData bundleStateData : tv.values()) {
            if (inFlightStates.contains(bundleStateData.getState())
                    && now - bundleStateData.getTimestamp() > MAX_IN_FLIGHT_STATE_WAITING_TIME_IN_MILLIS) {
                tombstoneAsync(bundleStateData.getBundle());
                releasedBundleCnt++;
            }
        }
        log.info("Completed old in-flight bundle cleanup. Released bundle count:{}", releasedBundleCnt);
    }

    public CompletableFuture<Optional<String>> getChannelOwnerBroker(ServiceUnitId topic) {
        Optional<LeaderBroker> leader = leaderElectionService.getCurrentLeader();
        if (leader.isPresent()) {
            String broker = leader.get().getServiceUrl();
            //expecting http://broker-xyz:abcd
            broker = broker.substring(broker.lastIndexOf('/') + 1);
            log.info("Found channelOwnerBroker:{} for topic:{}", broker, topic);
            return CompletableFuture.completedFuture(Optional.of(broker));
        } else {
            log.info(
                    "No leader elected from bundleStateChannelLeaderElectionService for topic:" + topic);
            throw new IllegalStateException(
                    "No leader elected from bundleStateChannelLeaderElectionService for topic:" + topic);
        }
    }
}
