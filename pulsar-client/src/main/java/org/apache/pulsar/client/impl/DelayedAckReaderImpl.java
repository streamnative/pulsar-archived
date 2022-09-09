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
package org.apache.pulsar.client.impl;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.impl.conf.ReaderConfigurationData;
import org.apache.pulsar.client.util.ExecutorProvider;
import org.apache.pulsar.common.api.proto.CommandAck;

@Slf4j
public class DelayedAckReaderImpl<T> extends ReaderImpl<T> {

    ConsumerBase<T> consumer;
    public DelayedAckReaderImpl(PulsarClientImpl client, ReaderConfigurationData<T> readerConfiguration,
                                ExecutorProvider executorProvider, CompletableFuture<Consumer<T>> consumerFuture,
                                Schema<T> schema) {
        super(client, readerConfiguration, executorProvider, consumerFuture, schema);
        this.consumer = getConsumer();
    }


    @Override
    public Message<T> readNext() throws PulsarClientException {

        Message<T> msg = consumer.receive();
        return msg;
    }

    @Override
    public Message<T> readNext(int timeout, TimeUnit unit) throws PulsarClientException {
        Message<T> msg = consumer.receive(timeout, unit);
        return msg;
    }

    @Override
    public CompletableFuture<Message<T>> readNextAsync() {
        CompletableFuture<Message<T>> receiveFuture = consumer.receiveAsync();
        return receiveFuture;
    }

    public CompletableFuture<MessageId> getLastMessageIdAsync() {
        return consumer.getLastMessageIdAsync();
    }

    public CompletableFuture<Void> acknowledgeCumulativeAsync(MessageId messageId, Map<String, Long> properties) {
        return consumer.doAcknowledgeWithTxn(messageId, CommandAck.AckType.Cumulative, properties, null);
    }
}
