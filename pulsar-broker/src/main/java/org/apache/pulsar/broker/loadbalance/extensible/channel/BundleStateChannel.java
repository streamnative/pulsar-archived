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
import org.apache.pulsar.broker.PulsarServerException;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.TableView;
import org.apache.pulsar.client.impl.schema.JSONSchema;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.naming.TopicDomain;
import org.apache.pulsar.common.util.collections.ConcurrentOpenHashMap;

@Slf4j
public class BundleStateChannel {

    private final TableView<BundleStateData> tv;

    private final Cache<String, String> ownershipCache;
    private final Producer<BundleStateData> producer;

    private final ConcurrentOpenHashMap<String, CompletableFuture<Optional<String>>>
            assigningBundles = ConcurrentOpenHashMap.<String,
                    CompletableFuture<Optional<String>>>newBuilder()
            .build();

    public static final String TOPIC =
            TopicDomain.persistent.value()
                    + "://"
                    + NamespaceName.SYSTEM_NAMESPACE
                    + "/bundle-state-channel";

    public BundleStateChannel(PulsarClient client)
            throws PulsarServerException {

        try {
            val schema = JSONSchema.of(BundleStateData.class);
            tv = client.newTableViewBuilder(schema)
                    .topic(TOPIC)
                    .create();
            producer = client.newProducer(schema)
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

        tv.forEachAndListen((key, value) -> handle(value));
    }

    private void handle(BundleStateData data) {
        val assigning = assigningBundles.get(data.getBundle());
        // TODO : Add state validation
        switch (data.state) {
            case Assigned -> handleAssigned(data);
            case Assigning -> handleAssigning(data);
            default -> {
                log.error("Failed to handle bundle state data:{}", data);
            }
        }
    }

    private void handleAssigned(BundleStateData data) {
        val assigning = assigningBundles.get(data.getBundle());
        if (assigning != null){
            assigning.complete(Optional.of(data.getBroker()));
        }
        assigningBundles.remove(data.getBundle());

        //TODO: remove log
        log.info("handled-Assigned:{}", data);
    }

    private void handleAssigning(BundleStateData data) {
        BundleStateData next = new BundleStateData(BundleState.Assigned, data.bundle, data.broker);
        ownershipCache.put(data.bundle, next.broker);
        pubAsync(next);
        assigningBundles.remove(data.getBundle());

        //TODO: remove log
        log.info("handled-Assigning:{}", data);
    }


    private CompletableFuture<MessageId> pubAsync(BundleStateData data) {
        return producer.newMessage()
                .key(data.getBundle())
                .value(data)
                .sendAsync();
    }

    public CompletableFuture<Optional<String>> getOwner(String bundle) {

        CompletableFuture<Optional<String>> future = new CompletableFuture<>();
        String owner = ownershipCache.getIfPresent(bundle);
        if (owner != null){
            future.complete(Optional.of(owner));
            return future;
        }
        BundleStateData data = tv.get(bundle);
        if (data == null) {
            return null;
        } else if (data.getState() == BundleState.Assigned) {
            future.complete(Optional.of(data.getBroker()));
            return future;
        } else if (data.state == BundleState.Assigning) {
            assigningBundles.computeIfAbsent(bundle, k -> future);
            return future;
        }
        return null;
    }

    public CompletableFuture<Optional<String>> assignBundle(String bundle, String broker) {
        CompletableFuture<Optional<String>> future = new CompletableFuture<>();
        assigningBundles.computeIfAbsent(bundle, k -> future);
        return pubAsync(new BundleStateData(BundleState.Assigning, bundle, broker))
                .thenCompose(x -> {
                    future.orTimeout(30, TimeUnit.SECONDS);
                    return future;
                }).exceptionally(e -> {
                    assigningBundles.remove(bundle);
                    future.complete(Optional.empty());
                    return Optional.empty();
                });

    }
}
