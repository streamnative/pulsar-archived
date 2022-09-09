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
package org.apache.pulsar.compaction;

import static org.apache.pulsar.broker.loadbalance.extensible.channel.BundleState.Assigned;
import static org.apache.pulsar.broker.loadbalance.extensible.channel.BundleState.Assigning;
import static org.apache.pulsar.broker.loadbalance.extensible.channel.BundleState.Unloading;
import static org.apache.pulsar.broker.loadbalance.extensible.channel.BundleState.isValidTransition;
import static org.mockito.Mockito.anyLong;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.netty.buffer.ByteBuf;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import lombok.Cleanup;
import lombok.SneakyThrows;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.client.api.OpenBuilder;
import org.apache.bookkeeper.mledger.AsyncCallbacks;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.ManagedLedgerInfo;
import org.apache.bookkeeper.mledger.Position;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.pulsar.broker.BrokerTestUtil;
import org.apache.pulsar.broker.auth.MockedPulsarServiceBaseTest;
import org.apache.pulsar.broker.loadbalance.extensible.channel.BundleState;
import org.apache.pulsar.broker.loadbalance.extensible.channel.BundleStateCompactionStrategy;
import org.apache.pulsar.broker.loadbalance.extensible.channel.BundleStateData;
import org.apache.pulsar.broker.namespace.NamespaceService;
import org.apache.pulsar.broker.service.Topic;
import org.apache.pulsar.broker.service.persistent.PersistentTopic;
import org.apache.pulsar.broker.service.persistent.SystemTopic;
import org.apache.pulsar.client.api.CompressionType;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.CryptoKeyReader;
import org.apache.pulsar.client.api.EncryptionKeyInfo;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.MessageRoutingMode;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.ProducerBuilder;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Reader;
import org.apache.pulsar.client.api.SubscriptionInitialPosition;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.client.impl.BatchMessageIdImpl;
import org.apache.pulsar.client.impl.schema.JSONSchema;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.common.policies.data.PersistentTopicInternalStats;
import org.apache.pulsar.common.policies.data.RetentionPolicies;
import org.apache.pulsar.common.policies.data.TenantInfoImpl;
import org.apache.pulsar.common.protocol.Markers;
import org.apache.pulsar.common.util.FutureUtil;
import org.awaitility.Awaitility;

import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Ignore;
import org.testng.annotations.Test;

@Test(groups = "flaky")
public class BundleStateCompactionTest extends MockedPulsarServiceBaseTest {
    private ScheduledExecutorService compactionScheduler;
    private BookKeeper bk;
    private JSONSchema<BundleStateData> schema;
    private BundleStateCompactionStrategy strategy;

    private BundleState testState0;
    private BundleState testState1;
    private BundleState testState2;
    private BundleState testState3;
    private BundleState testState4;

    private static Random RANDOM = new Random();

    private BundleStateData testVal(BundleState state, String broker) {
        return new BundleStateData(state, broker);
    }

    private BundleStateData testValue0(String broker) {
        BundleState to = nextValidState(testState0);
        testState0 = to;
        return new BundleStateData(to, broker);
    }

    private BundleStateData testValue1(String broker) {
        BundleState to = nextValidState(testState1);
        testState1 = to;
        return new BundleStateData(to, broker);
    }

    private BundleStateData testValue2(String broker) {
        BundleState to = nextValidState(testState2);
        testState2 = to;
        return new BundleStateData(to, broker);
    }

    private BundleStateData testValue3(String broker) {
        BundleState to = nextValidState(testState3);
        testState3 = to;
        return new BundleStateData(to, broker);
    }

    private BundleStateData testValue4(String broker) {
        BundleState to = nextValidState(testState4);
        testState4 = to;
        return new BundleStateData(to, broker);
    }

    private BundleStateData nextValidStateData(BundleStateData from, String broker) {
        BundleState to = nextValidState(testState4);
        if(from != null
                && (from.getState() == Assigned && to == Assigning)
                || (from.getState() == Assigning && to == Assigned)) {
            return new BundleStateData(to, from.getBroker(), from.getSourceBroker());
        }
        return new BundleStateData(to, broker);
    }

    private BundleState nextValidState(BundleState from) {
        var candidates = Arrays.stream(BundleState.values())
                .filter(to -> isValidTransition(from, to))
                .collect(Collectors.toList());
        if (candidates.size() == 0) {
            return null;
        }
        return candidates.get(RANDOM.nextInt(candidates.size()));
    }

    private BundleState nextInvalidState(BundleState from) {
        var candidates = Arrays.stream(BundleState.values())
                .filter(to -> !isValidTransition(from, to))
                .collect(Collectors.toList());
        if (candidates.size() == 0) {
            return null;
        }
        return candidates.get(RANDOM.nextInt(candidates.size()));
    }

    private List<BundleState> nextStatesToNull(BundleState from) {
        if (from == null) {
            return List.of();
        }
        return switch (from) {
            case Assigning -> List.of(Assigned, Unloading);
            case Assigned -> List.of(Unloading);
            case Splitting, Unloading -> List.of();
            default -> List.of();
        };
    }

    @BeforeMethod
    @Override
    public void setup() throws Exception {
        super.internalSetup();

        admin.clusters().createCluster("use", ClusterData.builder().serviceUrl(pulsar.getWebServiceAddress()).build());
        admin.tenants().createTenant("my-property",
                new TenantInfoImpl(Sets.newHashSet("appid1", "appid2"), Sets.newHashSet("use")));
        admin.namespaces().createNamespace("my-property/use/my-ns");

        compactionScheduler = Executors.newSingleThreadScheduledExecutor(
                new ThreadFactoryBuilder().setNameFormat("compaction-%d").setDaemon(true).build());
        bk = pulsar.getBookKeeperClientFactory().create(this.conf, null, null, Optional.empty(), null);
        schema = JSONSchema.of(BundleStateData.class);
        strategy = new BundleStateCompactionStrategy();
        strategy.checkBrokers(false);

    }

    @AfterMethod(alwaysRun = true)
    @Override
    public void cleanup() throws Exception {
        super.internalCleanup();

        if (compactionScheduler != null) {
            compactionScheduler.shutdownNow();
        }
    }

    @Test
    public void testCompaction() throws Exception {
        String topic = "persistent://my-property/use/my-ns/my-topic1";
        final int numMessages = 20;
        final int maxKeys = 5;


        Producer<BundleStateData> producer = pulsarClient.newProducer(schema)
                .topic(topic)
                .enableBatching(false)
                .messageRoutingMode(MessageRoutingMode.SinglePartition)
                .create();

        Map<String, BundleStateData> expected = new HashMap<>();
        List<Pair<String, BundleStateData>> all = new ArrayList<>();
        Random r = new Random(0);

        pulsarClient.newConsumer(schema)
                .topic(topic)
                .subscriptionName("sub1")
                .readCompacted(true)
                .subscribe().close();


        for (int j = 0; j < numMessages; j++) {
            int keyIndex = r.nextInt(maxKeys);
            String key = "key" + keyIndex;
            BundleStateData expectedData = expected.get(key);
            BundleState state = expectedData == null ? nextValidState(Assigning) :
                    r.nextInt(2) == 0 ? nextInvalidState(expectedData.getState()) :
                            nextValidState(expectedData.getState());
            BundleStateData value = new BundleStateData(state, key + ":" + j);
            producer.newMessage().key(key).value(value).send();
            BundleStateData prev = expected.get(key);
            if (strategy.isValid(prev, value)) {
                expected.put(key, value);
            }
            all.add(Pair.of(key, value));
        }

        StrategicTwoPhaseCompactor compactor
                = new StrategicTwoPhaseCompactor(conf, pulsarClient, bk, compactionScheduler);
        compactor.compact(topic, strategy).get();

        PersistentTopicInternalStats internalStats = admin.topics().getInternalStats(topic, false);
        // Compacted topic ledger should have same number of entry equals to number of unique key.
        Assert.assertEquals(expected.size(), internalStats.compactedLedger.entries);
        Assert.assertTrue(internalStats.compactedLedger.ledgerId > -1);
        Assert.assertFalse(internalStats.compactedLedger.offloaded);

        // consumer with readCompacted enabled only get compacted entries
        try (Consumer<BundleStateData> consumer = pulsarClient.newConsumer(schema).topic(topic).subscriptionName("sub1")
                .readCompacted(true).subscribe()) {
            while (true) {
                Message<BundleStateData> m = consumer.receive(2, TimeUnit.SECONDS);
                Assert.assertEquals(expected.remove(m.getKey()), m.getValue());
                if (expected.isEmpty()) {
                    break;
                }
            }
            Assert.assertTrue(expected.isEmpty());
        }

        // can get full backlog if read compacted disabled
        try (Consumer<BundleStateData> consumer = pulsarClient.newConsumer(schema).topic(topic).subscriptionName("sub1")
                .readCompacted(false).subscribe()) {
            while (true) {
                Message<BundleStateData> m = consumer.receive(2, TimeUnit.SECONDS);
                Pair<String, BundleStateData> expectedMessage = all.remove(0);
                Assert.assertEquals(expectedMessage.getLeft(), m.getKey());
                Assert.assertEquals(expectedMessage.getRight(), m.getValue());
                if (all.isEmpty()) {
                    break;
                }
            }
            Assert.assertTrue(all.isEmpty());
        }
    }

    @Test
    public void testCompactionWithReader() throws Exception {
        String topic = "persistent://my-property/use/my-ns/my-topic1";
        final int numMessages = 20;
        final int maxKeys = 5;

        // Configure retention to ensue data is retained for reader
        admin.namespaces().setRetention("my-property/use/my-ns", new RetentionPolicies(-1, -1));

        Producer<BundleStateData> producer = pulsarClient.newProducer(schema)
                .topic(topic)
                .enableBatching(false)
                .messageRoutingMode(MessageRoutingMode.SinglePartition)
                .create();

        Map<String, BundleStateData> expected = new HashMap<>();
        List<Pair<String, BundleStateData>> all = new ArrayList<>();
        Random r = new Random(0);

        for (int j = 0; j < numMessages; j++) {
            int keyIndex = r.nextInt(maxKeys);
            String key = "key" + keyIndex;
            BundleStateData expectedData = expected.get(key);
            BundleState state = expectedData == null ? nextValidState(Assigning) :
                    r.nextInt(2) == 0 ? nextInvalidState(expectedData.getState()) :
                            nextValidState(expectedData.getState());
            BundleStateData value = new BundleStateData(state, key + ":" + j);
            producer.newMessage().key(key).value(value).send();
            BundleStateData prev = expected.get(key);
            if (strategy.isValid(prev, value)) {
                expected.put(key, value);
            }
            all.add(Pair.of(key, value));
        }

        StrategicTwoPhaseCompactor compactor
                = new StrategicTwoPhaseCompactor(conf, pulsarClient, bk, compactionScheduler);
        compactor.compact(topic, strategy).get();

        // consumer with readCompacted enabled only get compacted entries
        try (Reader<BundleStateData> reader = pulsarClient.newReader(schema).topic(topic).readCompacted(true)
                .startMessageId(MessageId.earliest).create()) {
            while (true) {
                Message<BundleStateData> m = reader.readNext(2, TimeUnit.SECONDS);
                Assert.assertEquals(expected.remove(m.getKey()), m.getValue());
                if (expected.isEmpty()) {
                    break;
                }
            }
            Assert.assertTrue(expected.isEmpty());
        }

        // can get full backlog if read compacted disabled
        try (Reader<BundleStateData> reader = pulsarClient.newReader(schema).topic(topic).readCompacted(false)
                .startMessageId(MessageId.earliest).create()) {
            while (true) {
                Message<BundleStateData> m = reader.readNext(2, TimeUnit.SECONDS);
                Pair<String, BundleStateData> expectedMessage = all.remove(0);
                Assert.assertEquals(expectedMessage.getLeft(), m.getKey());
                Assert.assertEquals(expectedMessage.getRight(), m.getValue());
                if (all.isEmpty()) {
                    break;
                }
            }
            Assert.assertTrue(all.isEmpty());
        }
    }


    @Test
    public void testReadCompactedBeforeCompaction() throws Exception {
        String topic = "persistent://my-property/use/my-ns/my-topic1";

        Producer<BundleStateData> producer = pulsarClient.newProducer(schema)
                .topic(topic)
                .enableBatching(false)
                .create();

        pulsarClient.newConsumer(schema).topic(topic).subscriptionName("sub1").readCompacted(true).subscribe().close();

        producer.newMessage().key("key0").value(testValue0( "content0")).send();
        producer.newMessage().key("key0").value(testValue0("content1")).send();
        producer.newMessage().key("key0").value(testValue0( "content2")).send();

        try (Consumer<BundleStateData> consumer = pulsarClient.newConsumer(schema).topic(topic).subscriptionName("sub1")
                .readCompacted(true).subscribe()) {
            Message<BundleStateData> m = consumer.receive();
            Assert.assertEquals(m.getKey(), "key0");
            Assert.assertEquals(m.getValue().getBroker(), "content0");

            m = consumer.receive();
            Assert.assertEquals(m.getKey(), "key0");
            Assert.assertEquals(m.getValue().getBroker(), "content1");

            m = consumer.receive();
            Assert.assertEquals(m.getKey(), "key0");
            Assert.assertEquals(m.getValue().getBroker(), "content2");
        }

        StrategicTwoPhaseCompactor compactor
                = new StrategicTwoPhaseCompactor(conf, pulsarClient, bk, compactionScheduler);
        compactor.compact(topic, strategy).get();

        try (Consumer<BundleStateData> consumer = pulsarClient.newConsumer(schema).topic(topic).subscriptionName("sub1")
                .readCompacted(true).subscribe()) {
            Message<BundleStateData> m = consumer.receive();
            Assert.assertEquals(m.getKey(), "key0");
            Assert.assertEquals(m.getValue().getBroker(), "content2");
        }
    }

    @Test
    public void testReadEntriesAfterCompaction() throws Exception {
        String topic = "persistent://my-property/use/my-ns/my-topic1";

        Producer<BundleStateData> producer = pulsarClient.newProducer(schema)
                .topic(topic)
                .enableBatching(false)
                .create();

        pulsarClient.newConsumer(schema).topic(topic).subscriptionName("sub1").readCompacted(true).subscribe().close();

        producer.newMessage().key("key0").value(testValue0( "content0")).send();
        producer.newMessage().key("key0").value(testValue0("content1")).send();
        producer.newMessage().key("key0").value(testValue0( "content2")).send();

        StrategicTwoPhaseCompactor compactor
                = new StrategicTwoPhaseCompactor(conf, pulsarClient, bk, compactionScheduler);
        compactor.compact(topic, strategy).get();

        producer.newMessage().key("key0").value(testValue0("content3")).send();

        try (Consumer<BundleStateData> consumer = pulsarClient.newConsumer(schema).topic(topic).subscriptionName("sub1")
                .readCompacted(true).subscribe()) {
            Message<BundleStateData> m = consumer.receive();
            Assert.assertEquals(m.getKey(), "key0");
            Assert.assertEquals(m.getValue().getBroker(), "content2");

            m = consumer.receive();
            Assert.assertEquals(m.getKey(), "key0");
            Assert.assertEquals(m.getValue().getBroker(), "content3");
        }
    }

    @Test
    public void testSeekEarliestAfterCompaction() throws Exception {
        String topic = "persistent://my-property/use/my-ns/my-topic1";

        Producer<BundleStateData> producer = pulsarClient.newProducer(schema)
                .topic(topic)
                .enableBatching(false)
                .create();

        producer.newMessage().key("key0").value(testValue0( "content0")).send();
        producer.newMessage().key("key0").value(testValue0("content1")).send();
        producer.newMessage().key("key0").value(testValue0( "content2")).send();

        StrategicTwoPhaseCompactor compactor
                = new StrategicTwoPhaseCompactor(conf, pulsarClient, bk, compactionScheduler);
        compactor.compact(topic, strategy).get();

        try (Consumer<BundleStateData> consumer = pulsarClient.newConsumer(schema).topic(topic).subscriptionName("sub1")
                .readCompacted(true).subscribe()) {
            consumer.seek(MessageId.earliest);
            Message<BundleStateData> m = consumer.receive();
            Assert.assertEquals(m.getKey(), "key0");
            Assert.assertEquals(m.getValue().getBroker(), "content2");
        }

        try (Consumer<BundleStateData> consumer = pulsarClient.newConsumer(schema).topic(topic).subscriptionName("sub1")
                .readCompacted(false).subscribe()) {
            consumer.seek(MessageId.earliest);

            Message<BundleStateData> m = consumer.receive();
            Assert.assertEquals(m.getKey(), "key0");
            Assert.assertEquals(m.getValue().getBroker(), "content0");

            m = consumer.receive();
            Assert.assertEquals(m.getKey(), "key0");
            Assert.assertEquals(m.getValue().getBroker(), "content1");

            m = consumer.receive();
            Assert.assertEquals(m.getKey(), "key0");
            Assert.assertEquals(m.getValue().getBroker(), "content2");
        }
    }

    @Test
    public void testBrokerRestartAfterCompaction() throws Exception {
        String topic = "persistent://my-property/use/my-ns/my-topic1";

        Producer<BundleStateData> producer = pulsarClient.newProducer(schema)
                .topic(topic)
                .enableBatching(false)
                .create();

        pulsarClient.newConsumer(schema).topic(topic).subscriptionName("sub1").readCompacted(true).subscribe().close();

        producer.newMessage().key("key0").value(testValue0( "content0")).send();
        producer.newMessage().key("key0").value(testValue0("content1")).send();
        producer.newMessage().key("key0").value(testValue0( "content2")).send();

        StrategicTwoPhaseCompactor compactor
                = new StrategicTwoPhaseCompactor(conf, pulsarClient, bk, compactionScheduler);
        compactor.compact(topic, strategy).get();

        try (Consumer<BundleStateData> consumer = pulsarClient.newConsumer(schema).topic(topic).subscriptionName("sub1")
                .readCompacted(true).subscribe()) {
            Message<BundleStateData> m = consumer.receive();
            Assert.assertEquals(m.getKey(), "key0");
            Assert.assertEquals(m.getValue().getBroker(), "content2");
        }

        stopBroker();
        try (Consumer<BundleStateData> consumer = pulsarClient.newConsumer(schema).topic(topic).subscriptionName("sub1")
                .readCompacted(true).subscribe()) {
            consumer.receive();
            Assert.fail("Shouldn't have been able to receive anything");
        } catch (PulsarClientException e) {
            // correct behaviour
        }
        startBroker();

        try (Consumer<BundleStateData> consumer = pulsarClient.newConsumer(schema).topic(topic).subscriptionName("sub1")
                .readCompacted(true).subscribe()) {
            Message<BundleStateData> m = consumer.receive();
            Assert.assertEquals(m.getKey(), "key0");
            Assert.assertEquals(m.getValue().getBroker(), "content2");
        }
    }

    @Test
    public void testCompactEmptyTopic() throws Exception {
        String topic = "persistent://my-property/use/my-ns/my-topic1";

        Producer<BundleStateData> producer = pulsarClient.newProducer(schema)
                .topic(topic)
                .enableBatching(false)
                .create();

        pulsarClient.newConsumer(schema).topic(topic).subscriptionName("sub1").readCompacted(true).subscribe().close();

        new StrategicTwoPhaseCompactor(conf, pulsarClient, bk, compactionScheduler);

        producer.newMessage().key("key0").value(testValue0( "content0")).send();

        try (Consumer<BundleStateData> consumer = pulsarClient.newConsumer(schema).topic(topic).subscriptionName("sub1")
                .readCompacted(true).subscribe()) {
            Message<BundleStateData> m = consumer.receive();
            Assert.assertEquals(m.getKey(), "key0");
            Assert.assertEquals(m.getValue().getBroker(), "content0");
        }
    }

    @Test @Ignore
    public void testFirstMessageRetained() throws Exception {
        String topic = "persistent://my-property/use/my-ns/my-topic1";

        // subscribe before sending anything, so that we get all messages
        pulsarClient.newConsumer(schema).topic(topic).subscriptionName("sub1")
                .readCompacted(true).subscribe().close();

        try (Producer<BundleStateData> producer = pulsarClient.newProducer(schema).topic(topic).enableBatching(false)
                .create()) {
            producer.newMessage().key("key1").value(testValue1("my-message-1")).sendAsync();
            producer.newMessage().key("key2").value(testValue2( "my-message-2")).sendAsync();
            producer.newMessage().key("key2").value(testValue2("my-message-3")).send();
        }

        // Read messages before compaction to get ids
        Map<String, Message<BundleStateData>> expected = new HashMap<>();
        try (Consumer<BundleStateData> consumer = pulsarClient.newConsumer(schema).topic(topic)
                .subscriptionName("sub1").readCompacted(true).subscribe()) {
            for (int i = 0; i < 3; i++) {
                Message<BundleStateData> m = consumer.receive();
                expected.put(m.getKey(), m);
            }
        }

        // compact the topic
        StrategicTwoPhaseCompactor compactor
                = new StrategicTwoPhaseCompactor(conf, pulsarClient, bk, compactionScheduler);
        compactor.compact(topic, strategy).get();

        // Check that messages after compaction have same ids
        // BundleStateCompaction does not guarantee the order
        try (Consumer<BundleStateData> consumer = pulsarClient.newConsumer(schema).topic(topic)
                .subscriptionName("sub1").readCompacted(true).subscribe()) {
            Message<BundleStateData> message1 = consumer.receive();
            String key1 = message1.getKey();
            //Assert.assertEquals(message1.getKey(), expected.get(key));
            Assert.assertEquals(message1.getValue().getBroker(), expected.get(key1).getValue().getBroker());
            Assert.assertEquals(message1.getMessageId(), expected.remove(key1).getMessageId());

            Message<BundleStateData> message2 = consumer.receive();
            String key2 = message1.getKey();
            //Assert.assertEquals(message2.getKey(), "key2");
            Assert.assertEquals(message2.getValue().getBroker(), expected.get(key2).getValue().getBroker());
            Assert.assertEquals(message2.getMessageId(), expected.remove(key2).getMessageId());
        }
        Assert.assertTrue(expected.isEmpty());
    }

    @Test @Ignore
    public void testBatchMessageIdsDontChange() throws Exception {
        String topic = "persistent://my-property/use/my-ns/my-topic1";

        // subscribe before sending anything, so that we get all messages
        pulsarClient.newConsumer(schema).topic(topic).subscriptionName("sub1")
                .readCompacted(true).subscribe().close();

        try (Producer<BundleStateData> producer = pulsarClient.newProducer(schema).topic(topic)
                .maxPendingMessages(3)
                .enableBatching(true)
                .batchingMaxMessages(3)
                .batchingMaxPublishDelay(1, TimeUnit.HOURS)
                .messageRoutingMode(MessageRoutingMode.SinglePartition)
                .create()
        ) {
            producer.newMessage().key("key1").value(testValue1("my-message-1")).sendAsync();
            producer.newMessage().key("key2").value(testValue2( "my-message-2")).sendAsync();
            producer.newMessage().key("key2").value(testValue2("my-message-3")).send();
        }

        // Read messages before compaction to get ids
        List<Message<BundleStateData>> messages = new ArrayList<>();
        try (Consumer<BundleStateData> consumer = pulsarClient.newConsumer(schema).topic(topic)
                .subscriptionName("sub1").readCompacted(true).subscribe()) {
            messages.add(consumer.receive());
            messages.add(consumer.receive());
            messages.add(consumer.receive());
        }

        // Ensure all messages are in same batch
        Assert.assertEquals(((BatchMessageIdImpl) messages.get(0).getMessageId()).getLedgerId(),
                ((BatchMessageIdImpl) messages.get(1).getMessageId()).getLedgerId());
        Assert.assertEquals(((BatchMessageIdImpl) messages.get(0).getMessageId()).getLedgerId(),
                ((BatchMessageIdImpl) messages.get(2).getMessageId()).getLedgerId());
        Assert.assertEquals(((BatchMessageIdImpl) messages.get(0).getMessageId()).getEntryId(),
                ((BatchMessageIdImpl) messages.get(1).getMessageId()).getEntryId());
        Assert.assertEquals(((BatchMessageIdImpl) messages.get(0).getMessageId()).getEntryId(),
                ((BatchMessageIdImpl) messages.get(2).getMessageId()).getEntryId());

        // compact the topic
        StrategicTwoPhaseCompactor compactor
                = new StrategicTwoPhaseCompactor(conf, pulsarClient, bk, compactionScheduler);
        compactor.compact(topic, strategy).get();

        // Check that messages after compaction have same ids
        try (Consumer<BundleStateData> consumer = pulsarClient.newConsumer(schema).topic(topic)
                .subscriptionName("sub1").readCompacted(true).subscribe()) {
            Message<BundleStateData> message1 = consumer.receive();
            Assert.assertEquals(message1.getKey(), "key1");
            Assert.assertEquals(message1.getValue().getBroker(), "my-message-1");
            Assert.assertEquals(message1.getMessageId(), messages.get(0).getMessageId());

            Message<BundleStateData> message2 = consumer.receive();
            Assert.assertEquals(message2.getKey(), "key2");
            Assert.assertEquals(message2.getValue().getBroker(), "my-message-3");
            Assert.assertEquals(message2.getMessageId(), messages.get(2).getMessageId());
        }
    }

    @Test
    public void testWholeBatchCompactedOut() throws Exception {
        String topic = "persistent://my-property/use/my-ns/my-topic1";

        // subscribe before sending anything, so that we get all messages
        pulsarClient.newConsumer(schema).topic(topic).subscriptionName("sub1")
                .readCompacted(true).subscribe().close();

        try (Producer<BundleStateData> producerNormal = pulsarClient.newProducer(schema).topic(topic)
                .enableBatching(false)
                .messageRoutingMode(MessageRoutingMode.SinglePartition)
                .create();
             Producer<BundleStateData> producerBatch = pulsarClient.newProducer(schema).topic(topic)
                     .maxPendingMessages(3)
                     .enableBatching(true)
                     .batchingMaxMessages(3)
                     .batchingMaxPublishDelay(1, TimeUnit.HOURS)
                     .messageRoutingMode(MessageRoutingMode.SinglePartition)
                     .create()) {
            producerBatch.newMessage().key("key1").value(testValue1("my-message-1")).sendAsync();
            producerBatch.newMessage().key("key1").value(testValue1( "my-message-2")).sendAsync();
            producerBatch.newMessage().key("key1").value(testValue1("my-message-3")).sendAsync();
            producerNormal.newMessage().key("key1").value(testValue1( "my-message-4")).send();
        }

        // compact the topic
        StrategicTwoPhaseCompactor compactor
                = new StrategicTwoPhaseCompactor(conf, pulsarClient, bk, compactionScheduler);
        compactor.compact(topic, strategy).get();

        try (Consumer<BundleStateData> consumer = pulsarClient.newConsumer(schema).topic(topic)
                .subscriptionName("sub1").readCompacted(true).subscribe()) {
            Message<BundleStateData> message = consumer.receive();
            Assert.assertEquals(message.getKey(), "key1");
            Assert.assertEquals(new String(message.getValue().getBroker()), "my-message-4");
        }
    }

    @Test @Ignore
    public void testKeyLessMessagesPassThrough() throws Exception {
        String topic = "persistent://my-property/use/my-ns/my-topic1";

        // subscribe before sending anything, so that we get all messages
        pulsarClient.newConsumer(schema).topic(topic).subscriptionName("sub1")
                .readCompacted(true).subscribe().close();

        try (Producer<BundleStateData> producerNormal = pulsarClient.newProducer(schema).topic(topic).create();
             Producer<BundleStateData> producerBatch = pulsarClient.newProducer(schema).topic(topic)
                     .maxPendingMessages(3)
                     .enableBatching(true).batchingMaxMessages(3)
                     .batchingMaxPublishDelay(1, TimeUnit.HOURS).create()) {
            producerNormal.newMessage().value(testValue3("my-message-1")).send();

            producerBatch.newMessage().value(testValue3("my-message-2")).sendAsync();
            producerBatch.newMessage().key("key1").value(testValue1("my-message-3")).sendAsync();
            producerBatch.newMessage().key("key1").value(testValue1("my-message-4")).send();

            producerBatch.newMessage().key("key2").value(testValue2("my-message-5")).sendAsync();
            producerBatch.newMessage().key("key2").value(testValue2("my-message-6")).sendAsync();
            producerBatch.newMessage().value(testValue3("my-message-7")).send();

            producerNormal.newMessage().value(testValue3("my-message-8")).send();
        }

        // compact the topic
        StrategicTwoPhaseCompactor compactor
                = new StrategicTwoPhaseCompactor(conf, pulsarClient, bk, compactionScheduler);
        compactor.compact(topic, strategy).get();

        try (Consumer<BundleStateData> consumer = pulsarClient.newConsumer(schema).topic(topic)
                .subscriptionName("sub1").readCompacted(true).subscribe()) {
            Message<BundleStateData> message1 = consumer.receive();
            Assert.assertFalse(message1.hasKey());
            Assert.assertEquals(message1.getValue().getBroker(), "my-message-1");

            Message<BundleStateData> message2 = consumer.receive();
            Assert.assertFalse(message2.hasKey());
            Assert.assertEquals(message2.getValue().getBroker(), "my-message-2");

            Message<BundleStateData> message3 = consumer.receive();
            Assert.assertEquals(message3.getKey(), "key1");
            Assert.assertEquals(message3.getValue().getBroker(), "my-message-4");

            Message<BundleStateData> message4 = consumer.receive();
            Assert.assertEquals(message4.getKey(), "key2");
            Assert.assertEquals(message4.getValue().getBroker(), "my-message-6");

            Message<BundleStateData> message5 = consumer.receive();
            Assert.assertFalse(message5.hasKey());
            Assert.assertEquals(message5.getValue().getBroker(), "my-message-7");

            Message<BundleStateData> message6 = consumer.receive();
            Assert.assertFalse(message6.hasKey());
            Assert.assertEquals(message6.getValue().getBroker(), "my-message-8");
        }
    }


    @Test @Ignore
    public void testEmptyPayloadDeletes() throws Exception {
        String topic = "persistent://my-property/use/my-ns/my-topic1";

        // subscribe before sending anything, so that we get all messages
        pulsarClient.newConsumer(schema).topic(topic).subscriptionName("sub1")
                .readCompacted(true).subscribe().close();

        try (Producer<BundleStateData> producerNormal = pulsarClient.newProducer(schema)
                .topic(topic)
                .enableBatching(false)
                .create();
             Producer<BundleStateData> producerBatch = pulsarClient.newProducer(schema)
                     .topic(topic)
                     .maxPendingMessages(3)
                     .enableBatching(true)
                     .batchingMaxMessages(3)
                     .batchingMaxPublishDelay(1, TimeUnit.HOURS)
                     .create()) {

            // key0 persists through it all
            producerNormal.newMessage()
                    .key("key0")
                    .value(testValue0("my-message-0"))
                    .send();

            // key1 is added but then deleted
            producerNormal.newMessage()
                    .key("key1")
                    .value(testValue1("my-message-1"))
                    .send();

            producerNormal.newMessage()
                    .key("key1")
                    .send();

            // key2 is added but deleted in same batch
            producerBatch.newMessage()
                    .key("key2")
                    .value(testValue2("my-message-2"))
                    .sendAsync();
            producerBatch.newMessage()
                    .key("key3")
                    .value(testValue3("my-message-3"))
                    .sendAsync();
            producerBatch.newMessage()
                    .key("key2").send();

            // key3 is added in previous batch, deleted in this batch
            producerBatch.newMessage()
                    .key("key3")
                    .sendAsync();
            producerBatch.newMessage()
                    .key("key4")
                    .value(testValue4("my-message-3"))
                    .sendAsync();
            producerBatch.newMessage()
                    .key("key4")
                    .send();

            // key4 is added, deleted, then resurrected
            producerNormal.newMessage()
                    .key("key4")
                    .value(testValue4("my-message-4"))
                    .send();
        }

        // compact the topic
        StrategicTwoPhaseCompactor compactor
                = new StrategicTwoPhaseCompactor(conf, pulsarClient, bk, compactionScheduler);
        compactor.compact(topic, strategy).get();

        try (Consumer<BundleStateData> consumer = pulsarClient.newConsumer(schema).topic(topic)
                .subscriptionName("sub1").readCompacted(true).subscribe()) {
            Message<BundleStateData> message1 = consumer.receive();
            Assert.assertEquals(message1.getKey(), "key0");
            Assert.assertEquals(new String(message1.getValue().getBroker()), "my-message-0");

            Message<BundleStateData> message2 = consumer.receive();
            Assert.assertEquals(message2.getKey(), "key4");
            Assert.assertEquals(new String(message2.getValue().getBroker()), "my-message-4");
        }
    }

    @Test @Ignore
    public void testEmptyPayloadDeletesWhenCompressed() throws Exception {
        String topic = "persistent://my-property/use/my-ns/my-topic1";

        // subscribe before sending anything, so that we get all messages
        pulsarClient.newConsumer(schema).topic(topic).subscriptionName("sub1")
                .readCompacted(true).subscribe().close();

        try (Producer<BundleStateData> producerNormal = pulsarClient.newProducer(schema)
                .topic(topic)
                .enableBatching(false)
                .compressionType(CompressionType.LZ4)
                .create();
             Producer<BundleStateData> producerBatch = pulsarClient.newProducer(schema)
                     .topic(topic)
                     .maxPendingMessages(3)
                     .enableBatching(true)
                     .compressionType(CompressionType.LZ4)
                     .batchingMaxMessages(3)
                     .batchingMaxPublishDelay(1, TimeUnit.HOURS)
                     .create()) {

            // key0 persists through it all
            producerNormal.newMessage()
                    .key("key0")
                    .value(testValue0("my-message-0")).send();

            // key1 is added but then deleted
            producerNormal.newMessage()
                    .key("key1")
                    .value(testValue1("my-message-1")).send();

            producerNormal.newMessage()
                    .key("key1").send();

            // key2 is added but deleted in same batch
            producerBatch.newMessage()
                    .key("key2")
                    .value(testValue2("my-message-2")).sendAsync();
            producerBatch.newMessage()
                    .key("key3")
                    .value(testValue3("my-message-3")).sendAsync();
            producerBatch.newMessage()
                    .key("key2").send();

            // key3 is added in previous batch, deleted in this batch
            producerBatch.newMessage()
                    .key("key3")
                    .sendAsync();
            producerBatch.newMessage()
                    .key("key4")
                    .value(testValue4("my-message-3"))
                    .sendAsync();
            producerBatch.newMessage()
                    .key("key4")
                    .send();

            // key4 is added, deleted, then resurrected
            producerNormal.newMessage()
                    .key("key4")
                    .value(testValue4("my-message-4"))
                    .send();
        }

        // compact the topic
        StrategicTwoPhaseCompactor compactor
                = new StrategicTwoPhaseCompactor(conf, pulsarClient, bk, compactionScheduler);
        compactor.compact(topic, strategy).get();

        try (Consumer<BundleStateData> consumer = pulsarClient.newConsumer(schema).topic(topic)
                .subscriptionName("sub1").readCompacted(true).subscribe()) {
            Message<BundleStateData> message1 = consumer.receive();
            Assert.assertEquals(message1.getKey(), "key0");
            Assert.assertEquals(new String(message1.getValue().getBroker()), "my-message-0");

            Message<BundleStateData> message2 = consumer.receive();
            Assert.assertEquals(message2.getKey(), "key4");
            Assert.assertEquals(new String(message2.getValue().getBroker()), "my-message-4");
        }
    }

    // test compact no keys

    @Test @Ignore // FIXME : fix this test.
    public void testCompactorReadsCompacted() throws Exception {
        String topic = "persistent://my-property/use/my-ns/my-topic1";

        // capture opened ledgers
        Set<Long> ledgersOpened = Sets.newConcurrentHashSet();
        when(mockBookKeeper.newOpenLedgerOp()).thenAnswer(
                (invocation) -> {
                    OpenBuilder builder = (OpenBuilder) spy(invocation.callRealMethod());
                    when(builder.withLedgerId(anyLong())).thenAnswer(
                            (invocation2) -> {
                                ledgersOpened.add((Long) invocation2.getArguments()[0]);
                                return invocation2.callRealMethod();
                            });
                    return builder;
                });

        // subscribe before sending anything, so that we get all messages in sub1
        pulsarClient.newConsumer(schema).topic(topic).subscriptionName("sub1").subscribe().close();

        // create the topic on the broker
        try (Producer<BundleStateData> producerNormal = pulsarClient.newProducer(schema).topic(topic).create()) {
            producerNormal.newMessage()
                    .key("key0")
                    .value(testValue0("my-message-0"))
                    .send();
        }

        // force ledger roll
        pulsar.getBrokerService().getTopicReference(topic).get().close(false).get();

        // write a message to avoid issue #1517
        try (Producer<BundleStateData> producerNormal = pulsarClient.newProducer(schema).topic(topic).create()) {
            producerNormal.newMessage()
                    .key("key1")
                    .value(testValue1("my-message-1"))
                    .send();
        }

        // verify second ledger created
        String managedLedgerName = ((PersistentTopic) pulsar.getBrokerService().getTopicReference(topic).get())
                .getManagedLedger().getName();
        ManagedLedgerInfo info = pulsar.getManagedLedgerFactory().getManagedLedgerInfo(managedLedgerName);
        Assert.assertEquals(info.ledgers.size(), 2);
        Assert.assertTrue(ledgersOpened.isEmpty()); // no ledgers should have been opened

        // compact the topic
        StrategicTwoPhaseCompactor compactor
                = new StrategicTwoPhaseCompactor(conf, pulsarClient, bk, compactionScheduler);
        compactor.compact(topic, strategy).get();

        // should have opened all except last to read
        Assert.assertTrue(ledgersOpened.contains(info.ledgers.get(0).ledgerId));
        Assert.assertFalse(ledgersOpened.contains(info.ledgers.get(1).ledgerId));
        ledgersOpened.clear();

        // force broker to close resources for topic
        pulsar.getBrokerService().getTopicReference(topic).get().close(false).get();

        // write a message to avoid issue #1517
        try (Producer<BundleStateData> producerNormal = pulsarClient.newProducer(schema).topic(topic).create()) {
            producerNormal.newMessage()
                    .key("key2")
                    .value(testValue2("my-message-2"))
                    .send();
        }

        info = pulsar.getManagedLedgerFactory().getManagedLedgerInfo(managedLedgerName);
        Assert.assertEquals(info.ledgers.size(), 3);

        // should only have opened the penultimate ledger to get stat
        Assert.assertFalse(ledgersOpened.contains(info.ledgers.get(0).ledgerId));
        Assert.assertFalse(ledgersOpened.contains(info.ledgers.get(1).ledgerId));
        Assert.assertFalse(ledgersOpened.contains(info.ledgers.get(2).ledgerId));
        ledgersOpened.clear();

        // compact the topic again
        compactor.compact(topic, strategy).get();

        // shouldn't have opened first ledger (already compacted), penultimate would have some uncompacted data.
        // last ledger already open for writing
        Assert.assertFalse(ledgersOpened.contains(info.ledgers.get(0).ledgerId));
        Assert.assertTrue(ledgersOpened.contains(info.ledgers.get(1).ledgerId));
        Assert.assertFalse(ledgersOpened.contains(info.ledgers.get(2).ledgerId));

        // all three messages should be there when we read compacted
        try (Consumer<BundleStateData> consumer = pulsarClient.newConsumer(schema).topic(topic)
                .subscriptionName("sub1").readCompacted(true).subscribe()) {
            Message<BundleStateData> message1 = consumer.receive();
            Assert.assertEquals(message1.getKey(), "key0");
            Assert.assertEquals(new String(message1.getValue().getBroker()), "my-message-0");

            Message<BundleStateData> message2 = consumer.receive();
            Assert.assertEquals(message2.getKey(), "key1");
            Assert.assertEquals(new String(message2.getValue().getBroker()), "my-message-1");

            Message<BundleStateData> message3 = consumer.receive();
            Assert.assertEquals(message3.getKey(), "key2");
            Assert.assertEquals(new String(message3.getValue().getBroker()), "my-message-2");
        }
    }

    @Test
    public void testCompactCompressedNoBatch() throws Exception {
        String topic = "persistent://my-property/use/my-ns/my-topic1";

        // subscribe before sending anything, so that we get all messages
        pulsarClient.newConsumer(schema).topic(topic).subscriptionName("sub1")
                .readCompacted(true).subscribe().close();

        try (Producer<BundleStateData> producer = pulsarClient.newProducer(schema).topic(topic)
                .compressionType(CompressionType.LZ4).enableBatching(false).create()) {
            producer.newMessage()
                    .key("key1")
                    .value(testValue1("my-message-1"))
                    .sendAsync();
            producer.newMessage()
                    .key("key2")
                    .value(testValue2("my-message-2"))
                    .sendAsync();
            producer.newMessage()
                    .key("key2")
                    .value(testValue2("my-message-3"))
                    .send();
        }

        // compact the topic
        StrategicTwoPhaseCompactor compactor
                = new StrategicTwoPhaseCompactor(conf, pulsarClient, bk, compactionScheduler);
        compactor.compact(topic, strategy).get();

        try (Consumer<BundleStateData> consumer = pulsarClient.newConsumer(schema).topic(topic)
                .subscriptionName("sub1").readCompacted(true).subscribe()) {
            Message<BundleStateData> message1 = consumer.receive();
            Assert.assertEquals(message1.getKey(), "key1");
            Assert.assertEquals(new String(message1.getValue().getBroker()), "my-message-1");

            Message<BundleStateData> message2 = consumer.receive();
            Assert.assertEquals(message2.getKey(), "key2");
            Assert.assertEquals(new String(message2.getValue().getBroker()), "my-message-3");
        }
    }

    @Test @Ignore
    public void testCompactCompressedBatching() throws Exception {
        String topic = "persistent://my-property/use/my-ns/my-topic1";

        // subscribe before sending anything, so that we get all messages
        pulsarClient.newConsumer(schema).topic(topic).subscriptionName("sub1")
                .readCompacted(true).subscribe().close();

        try (Producer<BundleStateData> producer = pulsarClient.newProducer(schema).topic(topic)
                .compressionType(CompressionType.LZ4)
                .maxPendingMessages(3)
                .enableBatching(true)
                .batchingMaxMessages(3)
                .batchingMaxPublishDelay(1, TimeUnit.HOURS).create()) {
            producer.newMessage()
                    .key("key1")
                    .value(testValue1("my-message-1"))
                    .sendAsync();
            producer.newMessage()
                    .key("key2")
                    .value(testValue2("my-message-2"))
                    .sendAsync();
            producer.newMessage()
                    .key("key2")
                    .value(testValue2("my-message-3"))
                    .send();
        }

        // compact the topic
        StrategicTwoPhaseCompactor compactor
                = new StrategicTwoPhaseCompactor(conf, pulsarClient, bk, compactionScheduler);
        compactor.compact(topic, strategy).get();

        try (Consumer<BundleStateData> consumer = pulsarClient.newConsumer(schema).topic(topic)
                .subscriptionName("sub1").readCompacted(true).subscribe()) {
            Message<BundleStateData> message1 = consumer.receive();
            Assert.assertEquals(message1.getKey(), "key1");
            Assert.assertEquals(new String(message1.getValue().getBroker()), "my-message-1");

            Message<BundleStateData> message2 = consumer.receive();
            Assert.assertEquals(message2.getKey(), "key2");
            Assert.assertEquals(new String(message2.getValue().getBroker()), "my-message-3");
        }
    }

    class EncKeyReader implements CryptoKeyReader {
        EncryptionKeyInfo keyInfo = new EncryptionKeyInfo();

        @Override
        public EncryptionKeyInfo getPublicKey(String keyName, Map<String, String> keyMeta) {
            String CERT_FILE_PATH = "./src/test/resources/certificate/public-key." + keyName;
            if (Files.isReadable(Paths.get(CERT_FILE_PATH))) {
                try {
                    keyInfo.setKey(Files.readAllBytes(Paths.get(CERT_FILE_PATH)));
                    return keyInfo;
                } catch (IOException e) {
                    Assert.fail("Failed to read certificate from " + CERT_FILE_PATH);
                }
            } else {
                Assert.fail("Certificate file " + CERT_FILE_PATH + " is not present or not readable.");
            }
            return null;
        }

        @Override
        public EncryptionKeyInfo getPrivateKey(String keyName, Map<String, String> keyMeta) {
            String CERT_FILE_PATH = "./src/test/resources/certificate/private-key." + keyName;
            if (Files.isReadable(Paths.get(CERT_FILE_PATH))) {
                try {
                    keyInfo.setKey(Files.readAllBytes(Paths.get(CERT_FILE_PATH)));
                    return keyInfo;
                } catch (IOException e) {
                    Assert.fail("Failed to read certificate from " + CERT_FILE_PATH);
                }
            } else {
                Assert.fail("Certificate file " + CERT_FILE_PATH + " is not present or not readable.");
            }
            return null;
        }
    }

    @Test @Ignore
    public void testCompactEncryptedNoBatch() throws Exception {
        String topic = "persistent://my-property/use/my-ns/my-topic1";

        // subscribe before sending anything, so that we get all messages
        pulsarClient.newConsumer(schema).topic(topic).subscriptionName("sub1")
                .readCompacted(true).subscribe().close();

        try (Producer<BundleStateData> producer = pulsarClient.newProducer(schema).topic(topic)
                .addEncryptionKey("client-ecdsa.pem").cryptoKeyReader(new EncKeyReader())
                .enableBatching(false).create()) {
            producer.newMessage()
                    .key("key1")
                    .value(testValue1("my-message-1"))
                    .sendAsync();
            producer.newMessage()
                    .key("key2")
                    .value(testValue2("my-message-2"))
                    .sendAsync();
            producer.newMessage()
                    .key("key2")
                    .value(testValue2("my-message-3"))
                    .send();
        }

        // compact the topic
        StrategicTwoPhaseCompactor compactor
                = new StrategicTwoPhaseCompactor(conf, pulsarClient, bk, compactionScheduler);
        compactor.compact(topic, strategy).get();

        // Check that messages after compaction have same ids
        try (Consumer<BundleStateData> consumer = pulsarClient.newConsumer(schema).topic(topic)
                .subscriptionName("sub1").cryptoKeyReader(new EncKeyReader())
                .readCompacted(true).subscribe()) {
            Message<BundleStateData> message1 = consumer.receive();
            Assert.assertEquals(message1.getKey(), "key1");
            Assert.assertEquals(new String(message1.getValue().getBroker()), "my-message-1");

            Message<BundleStateData> message2 = consumer.receive();
            Assert.assertEquals(message2.getKey(), "key2");
            Assert.assertEquals(new String(message2.getValue().getBroker()), "my-message-3");
        }
    }

    @Test @Ignore
    public void testCompactEncryptedBatching() throws Exception {
        String topic = "persistent://my-property/use/my-ns/my-topic1";

        // subscribe before sending anything, so that we get all messages
        pulsarClient.newConsumer(schema).topic(topic).subscriptionName("sub1")
                .readCompacted(true).subscribe().close();

        try (Producer<BundleStateData> producer = pulsarClient.newProducer(schema).topic(topic)
                .addEncryptionKey("client-ecdsa.pem").cryptoKeyReader(new EncKeyReader())
                .maxPendingMessages(3)
                .enableBatching(true)
                .batchingMaxMessages(3)
                .batchingMaxPublishDelay(1, TimeUnit.HOURS).create()) {
            producer.newMessage()
                    .key("key1")
                    .value(testValue1("my-message-1"))
                    .sendAsync();
            producer.newMessage()
                    .key("key2")
                    .value(testValue2("my-message-2"))
                    .sendAsync();
            producer.newMessage()
                    .key("key2")
                    .value(testValue2("my-message-3"))
                    .send();
        }

        // compact the topic
        StrategicTwoPhaseCompactor compactor
                = new StrategicTwoPhaseCompactor(conf, pulsarClient, bk, compactionScheduler);
        compactor.compact(topic, strategy).get();

        // with encryption, all messages are passed through compaction as it doesn't
        // have the keys to decrypt the batch payload
        try (Consumer<BundleStateData> consumer = pulsarClient.newConsumer(schema).topic(topic)
                .subscriptionName("sub1").cryptoKeyReader(new EncKeyReader())
                .readCompacted(true).subscribe()) {
            Message<BundleStateData> message1 = consumer.receive();
            Assert.assertEquals(message1.getKey(), "key1");
            Assert.assertEquals(new String(message1.getValue().getBroker()), "my-message-1");

            Message<BundleStateData> message2 = consumer.receive();
            Assert.assertEquals(message2.getKey(), "key2");
            Assert.assertEquals(new String(message2.getValue().getBroker()), "my-message-2");

            Message<BundleStateData> message3 = consumer.receive();
            Assert.assertEquals(message3.getKey(), "key2");
            Assert.assertEquals(new String(message3.getValue().getBroker()), "my-message-3");
        }
    }

    @Test @Ignore
    public void testCompactEncryptedAndCompressedNoBatch() throws Exception {
        String topic = "persistent://my-property/use/my-ns/my-topic1";

        // subscribe before sending anything, so that we get all messages
        pulsarClient.newConsumer(schema).topic(topic).subscriptionName("sub1")
                .readCompacted(true).subscribe().close();

        try (Producer<BundleStateData> producer = pulsarClient.newProducer(schema).topic(topic)
                .addEncryptionKey("client-ecdsa.pem").cryptoKeyReader(new EncKeyReader())
                .compressionType(CompressionType.LZ4)
                .enableBatching(false).create()) {
            producer.newMessage()
                    .key("key1")
                    .value(testValue1("my-message-1"))
                    .sendAsync();
            producer.newMessage()
                    .key("key2")
                    .value(testValue2("my-message-2"))
                    .sendAsync();
            producer.newMessage()
                    .key("key2")
                    .value(testValue2("my-message-3"))
                    .send();
        }

        // compact the topic
        StrategicTwoPhaseCompactor compactor
                = new StrategicTwoPhaseCompactor(conf, pulsarClient, bk, compactionScheduler);
        compactor.compact(topic, strategy).get();

        // Check that messages after compaction have same ids
        try (Consumer<BundleStateData> consumer = pulsarClient.newConsumer(schema).topic(topic)
                .subscriptionName("sub1").cryptoKeyReader(new EncKeyReader())
                .readCompacted(true).subscribe()) {
            Message<BundleStateData> message1 = consumer.receive();
            Assert.assertEquals(message1.getKey(), "key1");
            Assert.assertEquals(new String(message1.getValue().getBroker()), "my-message-1");

            Message<BundleStateData> message2 = consumer.receive();
            Assert.assertEquals(message2.getKey(), "key2");
            Assert.assertEquals(new String(message2.getValue().getBroker()), "my-message-3");
        }
    }

    @Test @Ignore
    public void testCompactEncryptedAndCompressedBatching() throws Exception {
        String topic = "persistent://my-property/use/my-ns/my-topic1";

        // subscribe before sending anything, so that we get all messages
        pulsarClient.newConsumer(schema).topic(topic).subscriptionName("sub1")
                .readCompacted(true).subscribe().close();

        try (Producer<BundleStateData> producer = pulsarClient.newProducer(schema).topic(topic)
                .addEncryptionKey("client-ecdsa.pem").cryptoKeyReader(new EncKeyReader())
                .compressionType(CompressionType.LZ4)
                .maxPendingMessages(3)
                .enableBatching(true)
                .batchingMaxMessages(3)
                .batchingMaxPublishDelay(1, TimeUnit.HOURS).create()) {
            producer.newMessage()
                    .key("key1")
                    .value(testValue1("my-message-1"))
                    .sendAsync();
            producer.newMessage()
                    .key("key2")
                    .value(testValue2("my-message-2"))
                    .sendAsync();
            producer.newMessage()
                    .key("key2")
                    .value(testValue2("my-message-3"))
                    .send();
        }

        // compact the topic
        StrategicTwoPhaseCompactor compactor
                = new StrategicTwoPhaseCompactor(conf, pulsarClient, bk, compactionScheduler);
        compactor.compact(topic, strategy).get();

        // with encryption, all messages are passed through compaction as it doesn't
        // have the keys to decrypt the batch payload
        try (Consumer<BundleStateData> consumer = pulsarClient.newConsumer(schema).topic(topic)
                .subscriptionName("sub1").cryptoKeyReader(new EncKeyReader())
                .readCompacted(true).subscribe()) {
            Message<BundleStateData> message1 = consumer.receive();
            Assert.assertEquals(message1.getKey(), "key1");
            Assert.assertEquals(new String(message1.getValue().getBroker()), "my-message-1");

            Message<BundleStateData> message2 = consumer.receive();
            Assert.assertEquals(message2.getKey(), "key2");
            Assert.assertEquals(new String(message2.getValue().getBroker()), "my-message-2");

            Message<BundleStateData> message3 = consumer.receive();
            Assert.assertEquals(message3.getKey(), "key2");
            Assert.assertEquals(new String(message3.getValue().getBroker()), "my-message-3");
        }
    }

    @Test @Ignore
    public void testEmptyPayloadDeletesWhenEncrypted() throws Exception {
        String topic = "persistent://my-property/use/my-ns/my-topic1";

        // subscribe before sending anything, so that we get all messages
        pulsarClient.newConsumer(schema).topic(topic).subscriptionName("sub1")
                .readCompacted(true).subscribe().close();

        try (Producer<BundleStateData> producerNormal = pulsarClient.newProducer(schema)
                .topic(topic)
                .enableBatching(false)
                .addEncryptionKey("client-ecdsa.pem").cryptoKeyReader(new EncKeyReader())
                .create();
             Producer<BundleStateData> producerBatch = pulsarClient.newProducer(schema)
                     .topic(topic)
                     .maxPendingMessages(3)
                     .enableBatching(true)
                     .addEncryptionKey("client-ecdsa.pem").cryptoKeyReader(new EncKeyReader())
                     .batchingMaxMessages(3)
                     .batchingMaxPublishDelay(1, TimeUnit.HOURS)
                     .create()) {

            // key0 persists through it all
            producerNormal.newMessage()
                    .key("key0")
                    .value(testValue0("my-message-0")).send();

            // key1 is added but then deleted
            producerNormal.newMessage()
                    .key("key1")
                    .value(testValue1("my-message-1")).send();

            producerNormal.newMessage()
                    .key("key1")
                    .send();

            // key2 is added but deleted in same batch
            producerBatch.newMessage()
                    .key("key2")
                    .value(testValue2("my-message-2")).sendAsync();
            producerBatch.newMessage()
                    .key("key3")
                    .value(testValue3("my-message-3")).sendAsync();
            producerBatch.newMessage()
                    .key("key2").send();

            // key4 is added, deleted, then resurrected
            producerNormal.newMessage()
                    .key("key4")
                    .value(testValue4("my-message-4")).send();
        }

        // compact the topic
        StrategicTwoPhaseCompactor compactor
                = new StrategicTwoPhaseCompactor(conf, pulsarClient, bk, compactionScheduler);
        compactor.compact(topic, strategy).get();

        try (Consumer<BundleStateData> consumer = pulsarClient.newConsumer(schema).topic(topic)
                .cryptoKeyReader(new EncKeyReader())
                .subscriptionName("sub1").readCompacted(true).subscribe()) {
            Message<BundleStateData> message1 = consumer.receive();
            Assert.assertEquals(message1.getKey(), "key0");
            Assert.assertEquals(message1.getValue().getBroker(), "my-message-0");

            // see all messages from batch
            Message<BundleStateData> message2 = consumer.receive();
            Assert.assertEquals(message2.getKey(), "key2");
            Assert.assertEquals(message2.getValue().getBroker(), "my-message-2");

            Message<BundleStateData> message3 = consumer.receive();
            Assert.assertEquals(message3.getKey(), "key3");
            Assert.assertEquals(message3.getValue().getBroker(), "my-message-3");

            Message<BundleStateData> message4 = consumer.receive();
            Assert.assertEquals(message4.getKey(), "key2");
            Assert.assertEquals(message4.getValue().getBroker(), "");

            Message<BundleStateData> message5 = consumer.receive();
            Assert.assertEquals(message5.getKey(), "key4");
            Assert.assertEquals(message5.getValue().getBroker(), "my-message-4");
        }
    }

    @DataProvider(name = "lastDeletedBatching")
    public static Object[][] lastDeletedBatching() {
        return new Object[][]{{false}};
        // FIXME : enable batching if batching is supported
        //return new Object[][]{{true}, {false}};
    }

    @Test(timeOut = 20000, dataProvider = "lastDeletedBatching")
    public void testCompactionWithLastDeletedKey(boolean batching) throws Exception {
        String topic = "persistent://my-property/use/my-ns/my-topic1";

        Producer<BundleStateData> producer = pulsarClient.newProducer(schema).topic(topic).enableBatching(batching)
                .messageRoutingMode(MessageRoutingMode.SinglePartition).create();

        pulsarClient.newConsumer(schema).topic(topic).subscriptionName("sub1").readCompacted(true).subscribe().close();

        producer.newMessage().key("1").value(testVal(Assigned, "1")).send();
        producer.newMessage().key("1").value(testVal(Unloading, "2")).send();
        producer.newMessage().key("2").value(testVal(Assigned, "3")).send();
        producer.newMessage().key("2").value(testVal(Unloading, "4")).send();
        producer.newMessage().key("3").value(testVal(Assigned, "5")).send();
        producer.newMessage().key("1").value(null).send();
        producer.newMessage().key("2").value(null).send();

        StrategicTwoPhaseCompactor compactor
                = new StrategicTwoPhaseCompactor(conf, pulsarClient, bk, compactionScheduler);
        compactor.compact(topic, strategy).get();

        Set<String> expected = Sets.newHashSet("3");
        // consumer with readCompacted enabled only get compacted entries
        try (Consumer<BundleStateData> consumer = pulsarClient.newConsumer(schema).topic(topic).subscriptionName("sub1")
                .readCompacted(true).subscribe()) {
            Message<BundleStateData> m = consumer.receive(2, TimeUnit.SECONDS);
            assertTrue(expected.remove(m.getKey()));
        }
    }

    @Test(timeOut = 20000, dataProvider = "lastDeletedBatching")
    public void testEmptyCompactionLedger(boolean batching) throws Exception {
        String topic = "persistent://my-property/use/my-ns/my-topic1";

        Producer<BundleStateData> producer = pulsarClient.newProducer(schema).topic(topic).enableBatching(batching)
                .messageRoutingMode(MessageRoutingMode.SinglePartition).create();

        pulsarClient.newConsumer(schema).topic(topic).subscriptionName("sub1").readCompacted(true).subscribe().close();

        producer.newMessage().key("1").value(testVal(Assigned, "1")).send();
        producer.newMessage().key("1").value(testVal(Unloading, "2")).send();
        producer.newMessage().key("2").value(testVal(Assigned, "3")).send();
        producer.newMessage().key("2").value(testVal(Unloading, "4")).send();
        producer.newMessage().key("1").value(null).send();
        producer.newMessage().key("2").value(null).send();

        StrategicTwoPhaseCompactor compactor
                = new StrategicTwoPhaseCompactor(conf, pulsarClient, bk, compactionScheduler);
        compactor.compact(topic, strategy).get();

        // consumer with readCompacted enabled only get compacted entries
        try (Consumer<BundleStateData> consumer = pulsarClient.newConsumer(schema).topic(topic).subscriptionName("sub1")
                .readCompacted(true).subscribe()) {
            Message<BundleStateData> m = consumer.receive(2, TimeUnit.SECONDS);
            assertNull(m);
        }
    }

    @Test(timeOut = 20000, dataProvider = "lastDeletedBatching")
    public void testAllEmptyCompactionLedger(boolean batchEnabled) throws Exception {
        final String topic =
                "persistent://my-property/use/my-ns/testAllEmptyCompactionLedger" + UUID.randomUUID().toString();

        final int messages = 10;

        // 1.create producer and publish message to the topic.
        ProducerBuilder<BundleStateData> builder = pulsarClient.newProducer(schema).topic(topic);
        if (!batchEnabled) {
            builder.enableBatching(false);
        } else {
            builder.batchingMaxMessages(messages / 5);
        }

        Producer<BundleStateData> producer = builder.create();

        List<CompletableFuture<MessageId>> futures = new ArrayList<>(messages);
        for (int i = 0; i < messages; i++) {
            futures.add(producer.newMessage().key("1").value(null).sendAsync());
        }

        FutureUtil.waitForAll(futures).get();

        // 2.compact the topic.
        StrategicTwoPhaseCompactor compactor
                = new StrategicTwoPhaseCompactor(conf, pulsarClient, bk, compactionScheduler);
        compactor.compact(topic, strategy).get();

        // consumer with readCompacted enabled only get compacted entries
        try (Consumer<BundleStateData> consumer = pulsarClient.newConsumer(schema).topic(topic).subscriptionName("sub1")
                .readCompacted(true).subscriptionInitialPosition(SubscriptionInitialPosition.Earliest).subscribe()) {
            Message<BundleStateData> m = consumer.receive(2, TimeUnit.SECONDS);
            assertNull(m);
        }
    }

    @Test(timeOut = 20000) @Ignore
    public void testBatchAndNonBatchWithoutEmptyPayload()
            throws PulsarClientException, ExecutionException, InterruptedException {
        final String topic =
                "persistent://my-property/use/my-ns/testBatchAndNonBatchWithoutEmptyPayload" + UUID.randomUUID()
                        .toString();

        // 1.create producer and publish message to the topic.
        Producer<BundleStateData> producer = pulsarClient.newProducer(schema)
                .topic(topic)
                .enableBatching(true)
                .batchingMaxPublishDelay(1, TimeUnit.DAYS)
                .create();

        final String k1 = "k1";
        final String k2 = "k2";
        producer.newMessage().key(k1).value(testValue1("0")).send();
        List<CompletableFuture<MessageId>> futures = new ArrayList<>(7);
        for (int i = 0; i < 2; i++) {
            futures.add(producer.newMessage().key(k1).value(testValue1(i + 1 + "")).sendAsync());
        }
        producer.flush();
        producer.newMessage().key(k1).value(testValue1("3")).send();
        for (int i = 0; i < 2; i++) {
            futures.add(producer.newMessage().key(k1).value(testValue1((i + 4 + ""))).sendAsync());
        }
        producer.flush();

        for (int i = 0; i < 3; i++) {
            futures.add(producer.newMessage().key(k2).value(testValue2((i + ""))).sendAsync());
        }

        producer.newMessage().key(k2).value(testValue2("3")).send();
        producer.flush();
        FutureUtil.waitForAll(futures).get();

        // 2.compact the topic.
        StrategicTwoPhaseCompactor compactor
                = new StrategicTwoPhaseCompactor(conf, pulsarClient, bk, compactionScheduler);
        compactor.compact(topic, strategy).get();

        // consumer with readCompacted enabled only get compacted entries
        try (Consumer<BundleStateData> consumer = pulsarClient.newConsumer(schema).topic(topic).subscriptionName("sub1")
                .readCompacted(true).subscriptionInitialPosition(SubscriptionInitialPosition.Earliest).subscribe()) {
            Message<BundleStateData> m1 = consumer.receive(2, TimeUnit.SECONDS);
            Message<BundleStateData> m2 = consumer.receive(2, TimeUnit.SECONDS);
            assertNotNull(m1);
            assertNotNull(m2);
            assertEquals(m1.getKey(), k1);
            assertEquals(m1.getValue().getBroker(), "5");
            assertEquals(m2.getKey(), k2);
            assertEquals(m2.getValue().getBroker(), "3");
            Message<BundleStateData> none = consumer.receive(2, TimeUnit.SECONDS);
            assertNull(none);
        }
    }

    @Test(timeOut = 20000) @Ignore
    public void testBatchAndNonBatchWithEmptyPayload()
            throws PulsarClientException, ExecutionException, InterruptedException {
        final String topic =
                "persistent://my-property/use/my-ns/testBatchAndNonBatchWithEmptyPayload" + UUID.randomUUID()
                        .toString();

        // 1.create producer and publish message to the topic.
        Producer<BundleStateData> producer = pulsarClient.newProducer(schema)
                .topic(topic)
                .enableBatching(true)
                .batchingMaxPublishDelay(1, TimeUnit.DAYS)
                .create();

        final String k1 = "k1";
        final String k2 = "k2";
        final String k3 = "k3";
        producer.newMessage().key(k1).value(testValue1("0")).send();
        List<CompletableFuture<MessageId>> futures = new ArrayList<>(7);
        for (int i = 0; i < 2; i++) {
            futures.add(producer.newMessage().key(k1).value(testValue1((i + 1 + ""))).sendAsync());
        }
        producer.flush();
        producer.newMessage().key(k1).value(testValue1("3")).send();
        for (int i = 0; i < 2; i++) {
            futures.add(producer.newMessage().key(k1).value(testValue1((i + 4 + ""))).sendAsync());
        }
        producer.flush();

        for (int i = 0; i < 3; i++) {
            futures.add(producer.newMessage().key(k2).value(testValue2((i + 10 + ""))).sendAsync());
        }
        producer.flush();

        producer.newMessage().key(k2).value(testValue2("")).send();

        producer.newMessage().key(k3).value(testValue2("0")).send();

        FutureUtil.waitForAll(futures).get();

        // 2.compact the topic.
        StrategicTwoPhaseCompactor compactor
                = new StrategicTwoPhaseCompactor(conf, pulsarClient, bk, compactionScheduler);
        compactor.compact(topic, strategy).get();

        // consumer with readCompacted enabled only get compacted entries
        try (Consumer<BundleStateData> consumer = pulsarClient.newConsumer(schema).topic(topic).subscriptionName("sub1")
                .readCompacted(true).subscriptionInitialPosition(SubscriptionInitialPosition.Earliest).subscribe()) {
            Message<BundleStateData> m1 = consumer.receive();
            Message<BundleStateData> m2 = consumer.receive();
            assertNotNull(m1);
            assertNotNull(m2);
            assertEquals(m1.getKey(), k1);
            assertEquals(m2.getKey(), k3);
            assertEquals(m1.getValue().getBroker(), "5");
            assertEquals(m2.getValue().getBroker(), "0");
            Message<BundleStateData> none = consumer.receive(2, TimeUnit.SECONDS);
            assertNull(none);
        }
    }

    @Test(timeOut = 20000) @Ignore
    public void testBatchAndNonBatchEndOfEmptyPayload()
            throws PulsarClientException, ExecutionException, InterruptedException {
        final String topic =
                "persistent://my-property/use/my-ns/testBatchAndNonBatchWithEmptyPayload" + UUID.randomUUID()
                        .toString();

        // 1.create producer and publish message to the topic.
        Producer<BundleStateData> producer = pulsarClient.newProducer(schema)
                .topic(topic)
                .enableBatching(true)
                .batchingMaxPublishDelay(1, TimeUnit.DAYS)
                .create();

        final String k1 = "k1";
        final String k2 = "k2";
        producer.newMessage().key(k1).value(testValue1("0")).send();
        List<CompletableFuture<MessageId>> futures = new ArrayList<>(7);
        for (int i = 0; i < 2; i++) {
            futures.add(producer.newMessage().key(k1).value(testValue1((i + 1 + ""))).sendAsync());
        }
        producer.flush();
        producer.newMessage().key(k1).value(testValue1("3")).send();
        for (int i = 0; i < 2; i++) {
            futures.add(producer.newMessage().key(k1).value(testValue1((i + 4 + ""))).sendAsync());
        }
        producer.flush();

        for (int i = 0; i < 3; i++) {
            futures.add(producer.newMessage().key(k2).value(testValue2((i + 10 + ""))).sendAsync());
        }
        producer.flush();

        producer.newMessage().key(k2).value(testValue2("")).send();

        FutureUtil.waitForAll(futures).get();

        // 2.compact the topic.
        StrategicTwoPhaseCompactor compactor
                = new StrategicTwoPhaseCompactor(conf, pulsarClient, bk, compactionScheduler);
        compactor.compact(topic, strategy).get();

        // consumer with readCompacted enabled only get compacted entries
        try (Consumer<BundleStateData> consumer = pulsarClient.newConsumer(schema).topic(topic).subscriptionName("sub1")
                .readCompacted(true).subscriptionInitialPosition(SubscriptionInitialPosition.Earliest).subscribe()) {
            Message<BundleStateData> m1 = consumer.receive();
            assertNotNull(m1);
            assertEquals(m1.getKey(), k1);
            assertEquals(m1.getValue().getBroker(), "5");
            Message<BundleStateData> none = consumer.receive(2, TimeUnit.SECONDS);
            assertNull(none);
        }
    }

    @Test(timeOut = 20000, dataProvider = "lastDeletedBatching")
    public void testCompactMultipleTimesWithoutEmptyMessage(boolean batchEnabled)
            throws PulsarClientException, ExecutionException, InterruptedException {
        final String topic =
                "persistent://my-property/use/my-ns/testCompactMultipleTimesWithoutEmptyMessage" + UUID.randomUUID()
                        .toString();

        final int messages = 10;
        final String key = "1";

        // 1.create producer and publish message to the topic.
        ProducerBuilder<BundleStateData> builder = pulsarClient.newProducer(schema).topic(topic);
        if (!batchEnabled) {
            builder.enableBatching(false);
        } else {
            //builder.batchingMaxMessages(messages / 5);
        }

        Producer<BundleStateData> producer = builder.create();

        List<CompletableFuture<MessageId>> futures = new ArrayList<>(messages);
        for (int i = 0; i < messages; i++) {
            futures.add(producer.newMessage().key(key).value(testValue0((i + ""))).sendAsync());
        }

        FutureUtil.waitForAll(futures).get();

        // 2.compact the topic.
        StrategicTwoPhaseCompactor compactor
                = new StrategicTwoPhaseCompactor(conf, pulsarClient, bk, compactionScheduler);
        compactor.compact(topic, strategy).get();

        // 3. Send more ten messages
        futures.clear();
        for (int i = 0; i < messages; i++) {
            futures.add(producer.newMessage().key(key).value(testValue0((i + 10 + ""))).sendAsync());
        }
        FutureUtil.waitForAll(futures).get();

        // 4.compact again.
        compactor.compact(topic, strategy).get();

        try (Consumer<BundleStateData> consumer = pulsarClient.newConsumer(schema).topic(topic).subscriptionName("sub1")
                .readCompacted(true).subscriptionInitialPosition(SubscriptionInitialPosition.Earliest).subscribe()) {
            Message<BundleStateData> m1 = consumer.receive();
            assertNotNull(m1);
            assertEquals(m1.getKey(), key);
            assertEquals(m1.getValue().getBroker(), "19");
            Message<BundleStateData> none = consumer.receive(2, TimeUnit.SECONDS);
            assertNull(none);
        }
    }

    @Test(timeOut = 2000000, dataProvider = "lastDeletedBatching")
    public void testReadUnCompacted(boolean batchEnabled)
            throws PulsarClientException, ExecutionException, InterruptedException {
        final String topic = "persistent://my-property/use/my-ns/testReadUnCompacted" + UUID.randomUUID().toString();

        final int messages = 10;
        final String key = "1";

        // 1.create producer and publish message to the topic.
        ProducerBuilder<BundleStateData> builder = pulsarClient.newProducer(schema).topic(topic);
        if (!batchEnabled) {
            builder.enableBatching(false);
        } else {
            builder.batchingMaxMessages(messages / 5);
        }

        Producer<BundleStateData> producer = builder.create();

        List<CompletableFuture<MessageId>> futures = new ArrayList<>(messages);
        for (int i = 0; i < messages; i++) {
            futures.add(producer.newMessage().key(key).value(testValue0((i + ""))).sendAsync());
        }

        FutureUtil.waitForAll(futures).get();

        // 2.compact the topic.
        StrategicTwoPhaseCompactor compactor
                = new StrategicTwoPhaseCompactor(conf, pulsarClient, bk, compactionScheduler);
        compactor.compact(topic, strategy).get();

        // 3. Send more ten messages
        futures.clear();
        for (int i = 0; i < messages; i++) {
            futures.add(producer.newMessage().key(key).value(testValue0((i + 10 + ""))).sendAsync());
        }
        FutureUtil.waitForAll(futures).get();
        try (Consumer<BundleStateData> consumer = pulsarClient.newConsumer(schema)
                .topic(topic)
                .subscriptionName("sub1")
                .readCompacted(true)
                .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                .subscribe()) {
            for (int i = 0; i < 11; i++) {
                Message<BundleStateData> received = consumer.receive();
                assertNotNull(received);
                assertEquals(received.getKey(), key);
                assertEquals(received.getValue().getBroker(), i + 9 + "");
                consumer.acknowledge(received);
            }
            Message<BundleStateData> none = consumer.receive(2, TimeUnit.SECONDS);
            assertNull(none);
        }

        // 4.Send empty message to delete the key-value in the compacted topic.
        for (var state : nextStatesToNull(testState0)) {
            producer.newMessage().key(key).value(new BundleStateData(state, "xx")).send();
        }
        producer.newMessage().key(key).value(null).send();

        // 5.compact the topic.
        compactor.compact(topic, strategy).get();

        try (Consumer<BundleStateData> consumer = pulsarClient.newConsumer(schema)
                .topic(topic)
                .subscriptionName("sub2")
                .readCompacted(true)
                .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                .subscribe()) {
            Message<BundleStateData> none = consumer.receive(2, TimeUnit.SECONDS);
            assertNull(none);
        }

        for (int i = 0; i < messages; i++) {
            futures.add(producer.newMessage().key(key).value(testValue0((i + 20 + ""))).sendAsync());
        }
        FutureUtil.waitForAll(futures).get();

        try (Consumer<BundleStateData> consumer = pulsarClient.newConsumer(schema)
                .topic(topic)
                .subscriptionName("sub3")
                .readCompacted(true)
                .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
                .subscribe()) {
            for (int i = 0; i < 10; i++) {
                Message<BundleStateData> received = consumer.receive();
                assertNotNull(received);
                assertEquals(received.getKey(), key);
                assertEquals(received.getValue().getBroker(), i + 20 + "");
                consumer.acknowledge(received);
            }
            Message<BundleStateData> none = consumer.receive(2, TimeUnit.SECONDS);
            assertNull(none);
        }
    }

    @SneakyThrows
    @Test
    public void testHealthCheckTopicNotCompacted() {
        NamespaceName heartbeatNamespaceV1 =
                NamespaceService.getHeartbeatNamespace(pulsar.getAdvertisedAddress(), pulsar.getConfiguration());
        String topicV1 = "persistent://" + heartbeatNamespaceV1.toString() + "/healthcheck";
        NamespaceName heartbeatNamespaceV2 =
                NamespaceService.getHeartbeatNamespaceV2(pulsar.getAdvertisedAddress(), pulsar.getConfiguration());
        String topicV2 = heartbeatNamespaceV2.toString() + "/healthcheck";
        Producer<BundleStateData> producer1 = pulsarClient.newProducer(schema).topic(topicV1).create();
        Producer<BundleStateData> producer2 = pulsarClient.newProducer(schema).topic(topicV2).create();
        Optional<Topic> topicReferenceV1 = pulsar.getBrokerService().getTopic(topicV1, false).join();
        Optional<Topic> topicReferenceV2 = pulsar.getBrokerService().getTopic(topicV2, false).join();
        assertFalse(((SystemTopic) topicReferenceV1.get()).isCompactionEnabled());
        assertFalse(((SystemTopic) topicReferenceV2.get()).isCompactionEnabled());
        producer1.close();
        producer2.close();
    }

    @Test(timeOut = 60000)
    public void testCompactionWithMarker() throws Exception {
        String namespace = "my-property/use/my-ns";
        final TopicName dest = TopicName.get(
                BrokerTestUtil.newUniqueName("persistent://" + namespace + "/testWriteMarker"));
        admin.topics().createNonPartitionedTopic(dest.toString());
        @Cleanup
        Consumer<BundleStateData> consumer = pulsarClient.newConsumer(schema)
                .topic(dest.toString())
                .subscriptionName("test-compaction-sub")
                .subscriptionType(SubscriptionType.Exclusive)
                .readCompacted(true)
                .subscriptionInitialPosition(SubscriptionInitialPosition.Latest)
                .subscribe();
        @Cleanup
        Producer<BundleStateData> producer = pulsarClient.newProducer(schema)
                .topic(dest.toString())
                .enableBatching(false)
                .messageRoutingMode(MessageRoutingMode.SinglePartition)
                .create();
        producer.newMessage().value(testValue0(("msg-1"))).send();
        Optional<Topic> topic = pulsar.getBrokerService().getTopic(dest.toString(), true).join();
        Assert.assertTrue(topic.isPresent());
        PersistentTopic persistentTopic = (PersistentTopic) topic.get();
        Random random = new Random();
        for (int i = 0; i < 100; i++) {
            int rad = random.nextInt(3);
            ByteBuf marker;
            if (rad == 0) {
                marker = Markers.newTxnCommitMarker(-1L, 0, i);
            } else if (rad == 1) {
                marker = Markers.newTxnAbortMarker(-1L, 0, i);
            } else {
                marker = Markers.newReplicatedSubscriptionsSnapshotRequest(UUID.randomUUID().toString(), "r1");
            }
            persistentTopic.getManagedLedger().asyncAddEntry(marker, new AsyncCallbacks.AddEntryCallback() {
                @Override
                public void addComplete(Position position, ByteBuf entryData, Object ctx) {
                    //
                }

                @Override
                public void addFailed(ManagedLedgerException exception, Object ctx) {
                    //
                }
            }, null);
            marker.release();
        }
        producer.newMessage().value(testValue0(("msg-2"))).send();
        admin.topics().triggerCompaction(dest.toString());
        Awaitility.await()
                .atMost(50, TimeUnit.SECONDS)
                .pollInterval(1, TimeUnit.SECONDS)
                .untilAsserted(() -> {
                    long ledgerId = admin.topics().getInternalStats(dest.toString()).compactedLedger.ledgerId;
                    Assert.assertNotEquals(ledgerId, -1L);
                });
    }
}
