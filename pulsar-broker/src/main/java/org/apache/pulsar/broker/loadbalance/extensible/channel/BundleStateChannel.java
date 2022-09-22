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
import static org.apache.pulsar.broker.loadbalance.extensible.channel.BundleStateChannel.MetadataState.Jittery;
import static org.apache.pulsar.broker.loadbalance.extensible.channel.BundleStateChannel.MetadataState.Stable;
import static org.apache.pulsar.broker.loadbalance.extensible.channel.BundleStateChannel.MetadataState.Unstable;
import static org.apache.pulsar.metadata.api.extended.SessionEvent.SessionLost;
import static org.apache.pulsar.metadata.api.extended.SessionEvent.SessionReestablished;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.commons.lang3.StringUtils;
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
import org.apache.pulsar.metadata.api.extended.SessionEvent;

@Slf4j
public class BundleStateChannel {

    private TableView<BundleStateData> tv;

    private Producer<BundleStateData> producer;

    private final PulsarService pulsar;

    private final ConcurrentOpenHashMap<String, CompletableFuture<Optional<String>>>
            getOwnerRequests = ConcurrentOpenHashMap.<String,
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

    public static final long MAX_CLEAN_UP_DELAY_TIME_IN_SECS = 3 * 60; // 3 mins

    public static final long MIN_CLEAN_UP_DELAY_TIME_IN_SECS = 5; // 5 secs

    private String lookupServiceAddress;

    private LeaderElectionService leaderElectionService;

    private BrokerRegistry brokerRegistry;

    private boolean leaderCleanBundleOwnerships;

    private SessionEvent lastMetadataSessionEvent = SessionEvent.Reconnected;
    private long lastMetadataSessionEventTimestamp = 0;

    enum MetadataState {
        Stable,
        Jittery,
        Unstable
    }


    ConcurrentOpenHashMap<String, CompletableFuture<Void>> cleanupJobs =
            ConcurrentOpenHashMap.<String, CompletableFuture<Void>>newBuilder()
                    .build();

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

            pulsar.getLocalMetadataStore().registerSessionListener(this::handleMetadataSessionEvent);

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
            case Closed -> handleClosedState(bundle, data);
            case Splitting -> handleSplittingState(bundle, data);
            default -> throw new IllegalStateException("Failed to handle bundle state data:" + data);
        }
    }


    private static boolean isTransferCommand(BundleStateData data) {
        return StringUtils.isNotEmpty(data.getSourceBroker());
    }

    private static boolean isTransferCommand(Unload data) {
        return data.getDestBroker().isPresent();
    }

    private void handleAssignedState(String bundle, BundleStateData data) {
        val getOwnerRequest = getOwnerRequests.remove(bundle);
        if (getOwnerRequest != null) {
            getOwnerRequest.complete(Optional.of(data.getBroker()));
            // TODO: log the lookup delay time.
            log.info("{} returned deferred getOwnerRequest:{},{}", lookupServiceAddress, bundle, data);
        }
    }

    private void handleAssigningState(String bundle, BundleStateData data) {

        if (isTargetBroker(data.getBroker())) {
            BundleStateData next =
                    new BundleStateData(
                            isTransferCommand(data) ? BundleState.Closed : BundleState.Assigned,
                            data.getBroker(),
                            data.getSourceBroker());
            pubAsync(bundle, next);
            log.info("{} published :{},{},{}",
                    lookupServiceAddress, pulsar.getBrokerServiceUrl(), bundle, data);
        }

    }

    private void handleClosedState(String bundle, BundleStateData data) {

        if (isTargetBroker(data.getSourceBroker())) {
            // TODO: when close, pass message to clients to connect to the new broker
            closeBundle(bundle).thenAccept(x -> {
                log.info("{} closed bundle topics:{},{}", lookupServiceAddress, bundle, data);
                BundleStateData next =
                        new BundleStateData(
                                BundleState.Assigned,
                                data.getBroker(),
                                data.getSourceBroker());
                pubAsync(bundle, next);
                log.info("{} published :{},{},{}",
                        lookupServiceAddress, pulsar.getBrokerServiceUrl(), bundle, data);
            });
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
        closeBundle(bundle).thenAccept(
                x -> {
                    var request = getOwnerRequests.remove(bundle);
                    if (request != null) {
                        request.complete(Optional.empty());
                        log.info("{} returned deferred lookups:{}", lookupServiceAddress, bundle);
                    }
                }
        );

    }


    private CompletableFuture<MessageId> pubAsync(String bundle, BundleStateData data) {
        return producer.newMessage()
                .key(bundle)
                .value(data)
                .sendAsync()
                /*
                .thenApply(messageId -> {
                            log.info("Published message for bundle:{}, messageId:{}, data:{}",
                                    bundle, messageId, data);
                            return messageId;
                        }

                 )*/
                .exceptionally(e -> {
                    log.error("Failed to publish message for bundle:{}, data:{}", bundle, data, e);
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


    private CompletableFuture<Optional<String>> deferGetOwnerRequest(String bundle) {
        return getOwnerRequests
                .computeIfAbsent(bundle, k -> {
                    CompletableFuture future = new CompletableFuture<>();
                    future.orTimeout(MAX_IN_FLIGHT_STATE_WAITING_TIME_IN_MILLIS, TimeUnit.MILLISECONDS)
                            .whenComplete((v, e) -> getOwnerRequests.remove(bundle));
                    return future;
                });
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
            case Assigned, Splitting -> {
                return CompletableFuture.completedFuture(Optional.of(data.getBroker()));
            }
            case Assigning, Closed -> {
                return deferGetOwnerRequest(bundle);
            }
            default -> {
                return null;
            }
        }
    }

    public CompletableFuture<Optional<String>> publishAssignment(String bundle, String broker) {
        CompletableFuture<Optional<String>> getOwnerRequest = deferGetOwnerRequest(bundle);
        return pubAsync(bundle, new BundleStateData(Assigning, broker))
                .thenCompose(x -> getOwnerRequest)
                .exceptionally(e -> {
                    getOwnerRequests.remove(bundle);
                    getOwnerRequest.completeExceptionally(e);
                    return null;
                });
    }


    // TODO make it CompletableFuture
    public CompletableFuture<Void> publishUnload(Unload unload) {
        String bundle = unload.getBundle();
        if (isTransferCommand(unload)) {
            BundleStateData next = new BundleStateData(Assigning,
                    unload.getDestBroker().get(), unload.getSourceBroker());
            return pubAsync(bundle, next).thenAccept(__ -> {});
        }
        return tombstoneAsync(bundle).thenAccept(__ -> {});
    }

    public CompletableFuture<Void> splitBundle(Split split) {
        String bundle = split.getBundle();
        BundleStateData data = tv.get(bundle);
        BundleStateData next = new BundleStateData(BundleState.Splitting, data.getBroker());
        return pubAsync(bundle, next).thenAccept(__ -> {});
    }

    public void cleanBundleOwnerships(String broker) {
        cleanupJobs.remove(broker);

        if (!leaderElectionService.isLeader()) {
            return;
        }
        log.info("Started bundle ownership cleanup for the dead broker:{}", broker);
        int releasedBundleCnt = 0;
        for (Map.Entry<String, BundleStateData> etr : tv.entrySet()) {
            BundleStateData bundleStateData = etr.getValue();
            String bundle = etr.getKey();
            if (broker.equals(bundleStateData.getBroker())) {
                log.info("Unloading bundle ownership :{}", bundle);
                tombstoneAsync(bundle);
                releasedBundleCnt++;
            }
        }
        log.info("Completed bundle ownership cleanup. Released bundle count:{}",
                releasedBundleCnt);

        log.info("Active clean-up jobs count :{} after published tombstone", cleanupJobs.size());
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
        int releasedBundleCnt = 0;
        Set<String> deadBrokers = new HashSet<>();
        Set<String> activeBrokers = new HashSet<>(brokers);
        for (BundleStateData bundleStateData: tv.values()) {
            String broker = bundleStateData.getBroker();
            if (!activeBrokers.contains(broker)) {
                releasedBundleCnt++;
                deadBrokers.add(bundleStateData.getBroker());
            }
        }

        for (String deadBroker : deadBrokers) {
            handleDeadBroker(deadBroker);
        }

        log.info("Completed bundle ownership cleanup. Found Dead broker count:{}, Dead bundle count:{}",
                deadBrokers.size(), releasedBundleCnt);
    }

    public void cleanOldInFlightBundles() {
        if (!leaderElectionService.isLeader()) {
            return;
        }
        log.info("Started old in-flight bundle cleanup");
        int releasedBundleCnt = 0;
        long now = System.currentTimeMillis();
        Set<String> deadBrokers = new HashSet<>();
        for (BundleStateData bundleStateData: tv.values()) {
            if (inFlightStates.contains(bundleStateData.getState())
                    && now - bundleStateData.getTimestamp() > MAX_IN_FLIGHT_STATE_WAITING_TIME_IN_MILLIS) {
                deadBrokers.add(bundleStateData.getBroker());
                releasedBundleCnt++;
            }
        }

        for (String deadBroker : deadBrokers) {
            handleDeadBroker(deadBroker);
        }

        log.info("Completed old in-flight bundle ownership cleanup. "
                        + "Found Dead broker count:{}, Dead bundle count:{}",
                deadBrokers.size(), releasedBundleCnt);

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

    private MetadataState getMetadataState() {
        long now = System.currentTimeMillis();
        if (lastMetadataSessionEvent == SessionReestablished) {
            if (now - lastMetadataSessionEventTimestamp > 1000 * MAX_CLEAN_UP_DELAY_TIME_IN_SECS) {
                return Stable;
            }
            return Jittery;
        }
        return Unstable;
    }

    public void handleMetadataSessionEvent(SessionEvent e) {
        if(e == SessionReestablished || e == SessionLost){
            lastMetadataSessionEvent = e;
            lastMetadataSessionEventTimestamp = System.currentTimeMillis();
            log.info("Received metadata session event:{} at timestamp:{}",
                    lastMetadataSessionEvent, lastMetadataSessionEventTimestamp);
        }
    }

    public void handleBrokerCreationEvent(String broker) {
        if (!leaderElectionService.isLeader()) {
            return;
        }
        CompletableFuture<Void> future = cleanupJobs.remove(broker);
        if (future != null) {
            future.cancel(false);
            log.info("Successfully cancelled the bundle ownership clean-up for broker:{}", broker);
        } else {
            log.info("Failed to cancel the bundle ownership clean-up for broker:{}. "
                    + "There was no clean-up job.", broker);
        }

        log.info("Active clean-up job count :{} after trying the cancel", cleanupJobs.size());
    }


    public void handleDeadBroker(String broker) {
        if (!leaderElectionService.isLeader()) {
            return;
        }
        MetadataState state = getMetadataState();
        switch (state) {
            case Stable -> scheduleBundleOwnershipCleanUp(broker, MIN_CLEAN_UP_DELAY_TIME_IN_SECS);
            case Jittery -> scheduleBundleOwnershipCleanUp(broker, MAX_CLEAN_UP_DELAY_TIME_IN_SECS);
            case Unstable -> {
                log.error("MetadataState state is unstable. "
                        + "Ignoring the bundle ownership clean request for the reported broker :{} ", broker);
            }
        }
    }
    private void scheduleBundleOwnershipCleanUp(String broker, long delayInSecs) {
        if (!leaderElectionService.isLeader()) {
            return;
        }
        cleanupJobs.computeIfAbsent(broker, k -> {
            Executor delayed = CompletableFuture
                    .delayedExecutor(delayInSecs, TimeUnit.SECONDS, pulsar.getExecutor());
            return CompletableFuture
                    .runAsync(() -> cleanBundleOwnerships(broker), delayed);

        });
    }
}
