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

import io.netty.buffer.ByteBuf;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.Triple;
import org.apache.pulsar.client.api.CompressionType;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.common.api.proto.MessageIdData;
import org.apache.pulsar.common.api.proto.MessageMetadata;
import org.apache.pulsar.common.compression.CompressionCodecProvider;
import org.apache.pulsar.common.protocol.Commands;

@Slf4j
public class RawBatchMessageContainerImpl extends BatchMessageContainerImpl {
    private MessageIdData lastMessageId;


    public Triple<ByteBuf, MessageIdData, MessageMetadata> getPayloadAndMessageIdDataAndMetadata() {
        ByteBuf paylod = getCompressedBatchMetadataAndPayload();
        if (numMessagesInBatch > 1) {
            messageMetadata.setNumMessagesInBatch(numMessagesInBatch);
            messageMetadata.setSequenceId(lowestSequenceId);
            messageMetadata.setHighestSequenceId(highestSequenceId);
        }
        messageMetadata.setCompression(CompressionCodecProvider.convertToWireProtocol(CompressionType.NONE));
        messageMetadata.setUncompressedSize(paylod.readableBytes());
        return Triple.of(paylod, lastMessageId, messageMetadata);
    }

    @Override
    public boolean add(MessageImpl<?> msg, SendCallback callback) {
        MessageIdImpl msgId = (MessageIdImpl) msg.getMessageId();
        MessageIdData idData = new MessageIdData();
        idData.setEntryId(msgId.getEntryId());
        idData.setLedgerId(msgId.getLedgerId());
        idData.setPartition(msgId.getPartitionIndex());
        lastMessageId = idData;

        if (++numMessagesInBatch == 1) {
            try {
                // some properties are common amongst the different messages in the batch, hence we just pick it up from
                // the first message
                messageMetadata.setSequenceId(msg.getSequenceId());
                lowestSequenceId = Commands.initBatchMessageMetadata(messageMetadata, msg.getMessageBuilder());
                this.firstCallback = callback;
                batchedMessageMetadataAndPayload = allocator.compositeBuffer();
                if (msg.getMessageBuilder().hasTxnidMostBits() && currentTxnidMostBits == -1) {
                    currentTxnidMostBits = msg.getMessageBuilder().getTxnidMostBits();
                }
                if (msg.getMessageBuilder().hasTxnidLeastBits() && currentTxnidLeastBits == -1) {
                    currentTxnidLeastBits = msg.getMessageBuilder().getTxnidLeastBits();
                }
            } catch (Throwable e) {
                log.error("construct first message failed, exception is ", e);
                discard(new PulsarClientException(e));
                return false;
            }
        }

        if (previousCallback != null) {
            previousCallback.addCallback(msg, callback);
        }
        previousCallback = callback;
        currentBatchSizeBytes += msg.getDataBuffer().readableBytes();
        messages.add(msg);

        if (lowestSequenceId == -1L) {
            lowestSequenceId = msg.getSequenceId();
            messageMetadata.setSequenceId(lowestSequenceId);
        }
        highestSequenceId = msg.getSequenceId();
        //ProducerImpl.LAST_SEQ_ID_PUSHED_UPDATER.getAndUpdate(producer, prev -> Math.max(prev, msg.getSequenceId()));
        return isBatchFull();
    }

    protected ByteBuf getCompressedBatchMetadataAndPayload() {
        int batchWriteIndex = batchedMessageMetadataAndPayload.writerIndex();
        int batchReadIndex = batchedMessageMetadataAndPayload.readerIndex();

        for (int i = 0, n = messages.size(); i < n; i++) {
            MessageImpl<?> msg = messages.get(i);
            msg.getDataBuffer().markReaderIndex();
            try {
                if (n == 1) {
                    batchedMessageMetadataAndPayload.writeBytes(msg.getDataBuffer());
                } else  {
                    batchedMessageMetadataAndPayload = Commands.serializeSingleMessageInBatchWithPayload(
                            msg.getMessageBuilder(), msg.getDataBuffer(), batchedMessageMetadataAndPayload);
                }
                msg.release();
            } catch (Throwable th) {
                // serializing batch message can corrupt the index of message and batch-message. Reset the index so,
                // next iteration doesn't send corrupt message to broker.
                for (int j = 0; j <= i; j++) {
                    MessageImpl<?> previousMsg = messages.get(j);
                    previousMsg.getDataBuffer().resetReaderIndex();
                }
                batchedMessageMetadataAndPayload.writerIndex(batchWriteIndex);
                batchedMessageMetadataAndPayload.readerIndex(batchReadIndex);
                throw new RuntimeException(th);
            }
        }

        int uncompressedSize = batchedMessageMetadataAndPayload.readableBytes();

        // TODO: consider compression
        /*
        //ByteBuf compressedPayload = compressor.encode(batchedMessageMetadataAndPayload);
        //batchedMessageMetadataAndPayload.release();

        if (compressionType != CompressionType.NONE) {
            messageMetadata.setCompression(compressionType);
            messageMetadata.setUncompressedSize(uncompressedSize);
        }*/

        // Update the current max batch size using the uncompressed size, which is what we need in any case to
        // accumulate the batch content
        maxBatchSize = Math.max(maxBatchSize, uncompressedSize);
        return batchedMessageMetadataAndPayload;
    }


}
