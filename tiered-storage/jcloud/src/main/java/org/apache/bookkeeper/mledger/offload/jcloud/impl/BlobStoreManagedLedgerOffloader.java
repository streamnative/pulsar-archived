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
package org.apache.bookkeeper.mledger.offload.jcloud.impl;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.client.api.ReadHandle;
import org.apache.bookkeeper.common.util.OrderedScheduler;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.LedgerOffloader;
import org.apache.bookkeeper.mledger.LedgerOffloader.OffloadHandle.OfferEntryResult;
import org.apache.bookkeeper.mledger.ManagedCursor;
import org.apache.bookkeeper.mledger.ManagedLedger;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.impl.EntryImpl;
import org.apache.bookkeeper.mledger.impl.OffloadSegmentInfoImpl;
import org.apache.bookkeeper.mledger.impl.PositionImpl;
import org.apache.bookkeeper.mledger.offload.jcloud.BlockAwareSegmentInputStream;
import org.apache.bookkeeper.mledger.offload.jcloud.OffloadIndexBlock;
import org.apache.bookkeeper.mledger.offload.jcloud.OffloadIndexBlock.IndexInputStream;
import org.apache.bookkeeper.mledger.offload.jcloud.OffloadIndexBlockBuilder;
import org.apache.bookkeeper.mledger.offload.jcloud.OffloadIndexBlockV2;
import org.apache.bookkeeper.mledger.offload.jcloud.OffloadIndexBlockV2Builder;
import org.apache.bookkeeper.mledger.offload.jcloud.provider.BlobStoreLocation;
import org.apache.bookkeeper.mledger.offload.jcloud.provider.TieredStorageConfiguration;
import org.apache.bookkeeper.mledger.proto.MLDataFormats;
import org.apache.pulsar.common.policies.data.OffloadPolicies;
import org.jclouds.blobstore.BlobStore;
import org.jclouds.blobstore.domain.Blob;
import org.jclouds.blobstore.domain.BlobBuilder;
import org.jclouds.blobstore.domain.MultipartPart;
import org.jclouds.blobstore.domain.MultipartUpload;
import org.jclouds.blobstore.options.PutOptions;
import org.jclouds.domain.Location;
import org.jclouds.domain.LocationBuilder;
import org.jclouds.domain.LocationScope;
import org.jclouds.io.Payload;
import org.jclouds.io.Payloads;
import org.jclouds.io.payloads.InputStreamPayload;

/**
 * Tiered Storage Offloader that is backed by a JCloud Blob Store.
 * <p>
 * The constructor takes an instance of TieredStorageConfiguration, which
 * contains all of the configuration data necessary to connect to a JCloud
 * Provider service.
 * </p>
 */
@Slf4j
public class BlobStoreManagedLedgerOffloader implements LedgerOffloader {

    private final OrderedScheduler scheduler;
    private final TieredStorageConfiguration config;
    private final Location writeLocation;

    // metadata to be stored as part of the offloaded ledger metadata
    private final Map<String, String> userMetadata;

    private final ConcurrentMap<BlobStoreLocation, BlobStore> blobStores = new ConcurrentHashMap<>();
    private OffloadSegmentInfoImpl segmentInfo;
    private AtomicLong bufferLength = new AtomicLong(0);
    private AtomicLong segmentLength = new AtomicLong(0);
    final private long maxBufferLength;
    final private ConcurrentLinkedQueue<Entry> offloadBuffer = new ConcurrentLinkedQueue<>();
    private CompletableFuture<OffloadResult> offloadResult;
    private volatile PositionImpl lastOfferedPosition = PositionImpl.latest;
    private final Duration maxSegmentCloseTime;
    private final long minSegmentCloseTimeMillis;
    private final long segmentBeginTimeMillis;
    private final long maxSegmentLength;
    private final int streamingBlockSize;
    private volatile ManagedLedger ml;
    private OffloadIndexBlockV2Builder streamingIndexBuilder;

    public static BlobStoreManagedLedgerOffloader create(TieredStorageConfiguration config,
                                                         Map<String, String> userMetadata,
                                                         OrderedScheduler scheduler) throws IOException {

        return new BlobStoreManagedLedgerOffloader(config, scheduler, userMetadata);
    }

    BlobStoreManagedLedgerOffloader(TieredStorageConfiguration config, OrderedScheduler scheduler,
                                    Map<String, String> userMetadata) {

        this.scheduler = scheduler;
        this.userMetadata = userMetadata;
        this.config = config;
        this.streamingBlockSize = config.getMinBlockSizeInBytes();
        this.maxSegmentCloseTime = Duration.ofSeconds(config.getMaxSegmentTimeInSecond());
        this.maxSegmentLength = config.getMaxSegmentSizeInBytes();
        this.minSegmentCloseTimeMillis = Duration.ofSeconds(config.getMinSegmentTimeInSecond()).toMillis();
        //ensure buffer can have enough content to fill a block
        this.maxBufferLength = Math.max(config.getWriteBufferSizeInBytes(), config.getMinBlockSizeInBytes());
        this.segmentBeginTimeMillis = System.currentTimeMillis();

        if (!Strings.isNullOrEmpty(config.getRegion())) {
            this.writeLocation = new LocationBuilder()
                    .scope(LocationScope.REGION)
                    .id(config.getRegion())
                    .description(config.getRegion())
                    .build();
        } else {
            this.writeLocation = null;
        }

        log.info("Constructor offload driver: {}, host: {}, container: {}, region: {} ",
                config.getProvider().getDriver(), config.getServiceEndpoint(),
                config.getBucket(), config.getRegion());

        blobStores.putIfAbsent(config.getBlobStoreLocation(), config.getBlobStore());
        log.info("The ledger offloader was created.");
    }

    @Override
    public String getOffloadDriverName() {
        return config.getDriver();
    }

    @Override
    public Map<String, String> getOffloadDriverMetadata() {
        return config.getOffloadDriverMetadata();
    }

    /**
     * Upload the DataBlocks associated with the given ReadHandle using MultiPartUpload,
     * Creating indexBlocks for each corresponding DataBlock that is uploaded.
     */
    @Override
    public CompletableFuture<Void> offload(ReadHandle readHandle,
                                           UUID uuid,
                                           Map<String, String> extraMetadata) {
        final BlobStore writeBlobStore = blobStores.get(config.getBlobStoreLocation());
        CompletableFuture<Void> promise = new CompletableFuture<>();
        scheduler.chooseThread(readHandle.getId()).submit(() -> {
            if (readHandle.getLength() == 0 || !readHandle.isClosed() || readHandle.getLastAddConfirmed() < 0) {
                promise.completeExceptionally(
                        new IllegalArgumentException("An empty or open ledger should never be offloaded"));
                return;
            }
            OffloadIndexBlockBuilder indexBuilder = OffloadIndexBlockBuilder.create()
                .withLedgerMetadata(readHandle.getLedgerMetadata())
                .withDataBlockHeaderLength(BlockAwareSegmentInputStreamImpl.getHeaderSize());
            String dataBlockKey = DataBlockUtils.dataBlockOffloadKey(readHandle.getId(), uuid);
            String indexBlockKey = DataBlockUtils.indexBlockOffloadKey(readHandle.getId(), uuid);

            MultipartUpload mpu = null;
            List<MultipartPart> parts = Lists.newArrayList();

            // init multi part upload for data block.
            try {
                BlobBuilder blobBuilder = writeBlobStore.blobBuilder(dataBlockKey);
                DataBlockUtils.addVersionInfo(blobBuilder, userMetadata);
                Blob blob = blobBuilder.build();
                mpu = writeBlobStore.initiateMultipartUpload(config.getBucket(), blob.getMetadata(), new PutOptions());
            } catch (Throwable t) {
                promise.completeExceptionally(t);
                return;
            }

            long dataObjectLength = 0;
            // start multi part upload for data block.
            try {
                long startEntry = 0;
                int partId = 1;
                long entryBytesWritten = 0;
                while (startEntry <= readHandle.getLastAddConfirmed()) {
                    int blockSize = BlockAwareSegmentInputStreamImpl
                        .calculateBlockSize(config.getMaxBlockSizeInBytes(), readHandle, startEntry, entryBytesWritten);

                    try (BlockAwareSegmentInputStream blockStream = new BlockAwareSegmentInputStreamImpl(
                        readHandle, startEntry, blockSize)) {

                        Payload partPayload = Payloads.newInputStreamPayload(blockStream);
                        partPayload.getContentMetadata().setContentLength((long) blockSize);
                        partPayload.getContentMetadata().setContentType("application/octet-stream");
                        parts.add(writeBlobStore.uploadMultipartPart(mpu, partId, partPayload));
                        log.debug("UploadMultipartPart. container: {}, blobName: {}, partId: {}, mpu: {}",
                                config.getBucket(), dataBlockKey, partId, mpu.id());

                        indexBuilder.addBlock(startEntry, partId, blockSize);

                        if (blockStream.getEndEntryId() != -1) {
                            startEntry = blockStream.getEndEntryId() + 1;
                        } else {
                            // could not read entry from ledger.
                            break;
                        }
                        entryBytesWritten += blockStream.getBlockEntryBytesCount();
                        partId++;
                    }

                    dataObjectLength += blockSize;
                }

                writeBlobStore.completeMultipartUpload(mpu, parts);
                mpu = null;
            } catch (Throwable t) {
                try {
                    if (mpu != null) {
                        writeBlobStore.abortMultipartUpload(mpu);
                    }
                } catch (Throwable throwable) {
                    log.error("Failed abortMultipartUpload in bucket - {} with key - {}, uploadId - {}.",
                            config.getBucket(), dataBlockKey, mpu.id(), throwable);
                }
                promise.completeExceptionally(t);
                return;
            }

            // upload index block
            try (OffloadIndexBlock index = indexBuilder.withDataObjectLength(dataObjectLength).build();
                 IndexInputStream indexStream = index.toStream()) {
                // write the index block
                BlobBuilder blobBuilder = writeBlobStore.blobBuilder(indexBlockKey);
                DataBlockUtils.addVersionInfo(blobBuilder, userMetadata);
                Payload indexPayload = Payloads.newInputStreamPayload(indexStream);
                indexPayload.getContentMetadata().setContentLength((long) indexStream.getStreamSize());
                indexPayload.getContentMetadata().setContentType("application/octet-stream");

                Blob blob = blobBuilder
                        .payload(indexPayload)
                        .contentLength((long) indexStream.getStreamSize())
                    .build();

                writeBlobStore.putBlob(config.getBucket(), blob);
                promise.complete(null);
            } catch (Throwable t) {
                try {
                    writeBlobStore.removeBlob(config.getBucket(), dataBlockKey);
                } catch (Throwable throwable) {
                    log.error("Failed deleteObject in bucket - {} with key - {}.",
                            config.getBucket(), dataBlockKey, throwable);
                }
                promise.completeExceptionally(t);
                return;
            }
        });
        return promise;
    }

    @Override
    public CompletableFuture<OffloadResultV2> offloadV2(ManagedCursor cursor,
                                                        String managedLedgerName,
                                                        OffloadOption option) {
        assert option.offloadMethod == OffloadMethod.LEGER_BASED;
        //TODO is it a suitable value when we can't know the actual entry size?
        final int entriesPerRead = 100;
        final Long ledgerId = option.ledgerId;
        final BlobStore writeBlobStore = blobStores.get(config.getBlobStoreLocation());
        CompletableFuture<OffloadResultV2> promise = new CompletableFuture<>();
        String offloadKey = ledgerOffloadKey(managedLedgerName, ledgerId);
        String indexKey = ledgerOffloadIndexKey(managedLedgerName, ledgerId);
        scheduler.chooseThread(ledgerId).submit(() -> {
            try {
                final List<Entry> entriesForOffload = new LinkedList<>();
                int entriesByteLength = 0;
                long dataObjectLength = 0;
                cursor.resetCursor(PositionImpl.get(ledgerId, 0));
                Blob blob = writeBlobStore.blobBuilder(offloadKey).build();
                int offloadPartId = 1;
                List<MultipartPart> offloadParts = Lists.newArrayList();
                final OffloadIndexBlockV2Builder indexBuilder = OffloadIndexBlockV2Builder.create();
                final MultipartUpload mpu = blobStore
                        .initiateMultipartUpload(config.getBucket(), blob.getMetadata(), new PutOptions());
                long lastEntryId = 0;
                while (true) {
                    final List<Entry> readEntries = cursor.readEntries(entriesPerRead);
                    boolean finishRead = false;
                    for (Entry entry : readEntries) {
                        if (entry.getPosition().getLedgerId() > ledgerId) {
                            finishRead = true;
                            entry.release();
                            continue;
                        }
                        lastEntryId = entry.getPosition().getEntryId();
                        entriesForOffload.add(entry);
                        entriesByteLength += entry.getLength();
                        if (entriesByteLength >= config.getMaxBlockSizeInBytes() && !entriesForOffload.isEmpty()) {
                            //todo build and uploadpart
                            final int blockSize = BufferedOffloadStream
                                    .calculateBlockSize(streamingBlockSize, entriesForOffload.size(),
                                            entriesByteLength);
                            final Entry headEntry = entriesForOffload.get(0);
                            buildBlockAndUpload(blockSize, entriesForOffload, headEntry.getLedgerId(),
                                    headEntry.getEntryId(),
                                    offloadPartId, offloadParts, indexBuilder, blobStore);
                            dataObjectLength += blockSize;
                            assert entriesForOffload.isEmpty();
                            entriesByteLength = 0;
                            offloadPartId += 1;
                        }
                    }
                    if (finishRead) {
                        break;
                    }
                }
                if (!entriesForOffload.isEmpty()) {
                    final int blockSize = BufferedOffloadStream
                            .calculateBlockSize(streamingBlockSize, entriesForOffload.size(),
                                    entriesByteLength);
                    final Entry headEntry = entriesForOffload.get(0);
                    buildBlockAndUpload(blockSize, entriesForOffload, headEntry.getLedgerId(),
                            headEntry.getEntryId(),
                            offloadPartId, offloadParts, indexBuilder, blobStore);
                    dataObjectLength += blockSize;
                }
                buildIndexAndCompleteResult(dataObjectLength, blobStore, indexBuilder, mpu, offloadParts, indexKey);
                promise.complete(new OffloadResultV2(PositionImpl.get(ledgerId, lastEntryId),
                        PositionImpl.get(ledgerId, lastEntryId)));
            } catch (Throwable e) {
                promise.completeExceptionally(e);
            }
        });

        return promise;
    }

    private String ledgerOffloadKey(String managedLedgerName, long ledgerId) {
        return String.format("%s/data/%d", managedLedgerName, ledgerId);
    }

    private String ledgerOffloadIndexKey(String managedLedgerName, long ledgerId) {
        return String.format("%s/index/%d", managedLedgerName, ledgerId);
    }


    BlobStore blobStore;
    String streamingDataBlockKey;
    String streamingDataIndexKey;
    MultipartUpload streamingMpu = null;
    List<MultipartPart> streamingParts = Lists.newArrayList();

    @Override
    public CompletableFuture<OffloadHandle> streamingOffload(@NonNull ManagedLedger ml, UUID uuid, long beginLedger,
                                                             long beginEntry,
                                                             Map<String, String> driverMetadata) {
        if (this.ml != null) {
            log.error("streamingOffload should only be called once");
            final CompletableFuture<OffloadHandle> result = new CompletableFuture<>();
            result.completeExceptionally(new RuntimeException("streamingOffload should only be called once"));
        }

        this.ml = ml;
        this.segmentInfo = new OffloadSegmentInfoImpl(uuid, beginLedger, beginEntry, config.getDriver(),
                driverMetadata);
        log.debug("begin offload with {}:{}", beginLedger, beginEntry);
        this.offloadResult = new CompletableFuture<>();
        blobStore = blobStores.get(config.getBlobStoreLocation());
        streamingIndexBuilder = OffloadIndexBlockV2Builder.create();
        streamingDataBlockKey = segmentInfo.uuid.toString();
        streamingDataIndexKey = String.format("%s-index", segmentInfo.uuid);
        BlobBuilder blobBuilder = blobStore.blobBuilder(streamingDataBlockKey);
        DataBlockUtils.addVersionInfo(blobBuilder, userMetadata);
        Blob blob = blobBuilder.build();
        streamingMpu = blobStore
                .initiateMultipartUpload(config.getBucket(), blob.getMetadata(), new PutOptions());

        scheduler.chooseThread(segmentInfo).execute(() -> {
            log.info("start offloading segment: {}", segmentInfo);
            streamingOffloadLoop(1, 0);
        });
        scheduler.schedule(this::closeSegment, maxSegmentCloseTime.toMillis(), TimeUnit.MILLISECONDS);

        return CompletableFuture.completedFuture(new OffloadHandle() {
            @Override
            public Position lastOffered() {
                return BlobStoreManagedLedgerOffloader.this.lastOffered();
            }

            @Override
            public CompletableFuture<Position> lastOfferedAsync() {
                return CompletableFuture.completedFuture(lastOffered());
            }

            @Override
            public OfferEntryResult offerEntry(Entry entry) {
                return BlobStoreManagedLedgerOffloader.this.offerEntry(entry);
            }

            @Override
            public CompletableFuture<OfferEntryResult> offerEntryAsync(Entry entry) {
                return CompletableFuture.completedFuture(offerEntry(entry));
            }

            @Override
            public CompletableFuture<OffloadResult> getOffloadResultAsync() {
                return BlobStoreManagedLedgerOffloader.this.getOffloadResultAsync();
            }

            @Override
            public boolean close() {
                return BlobStoreManagedLedgerOffloader.this.closeSegment();
            }
        });
    }

    private void streamingOffloadLoop(int partId, int dataObjectLength) {
        log.debug("streaming offload loop {} {}", partId, dataObjectLength);
        if (segmentInfo.isClosed() && offloadBuffer.isEmpty()) {
            try {
                buildIndexAndCompleteResult(dataObjectLength, blobStore, streamingIndexBuilder, streamingMpu,
                        streamingParts, streamingDataIndexKey);
            } catch (IOException e) {
                offloadResult.completeExceptionally(e);
            }
            offloadResult.complete(segmentInfo.result());
        } else if ((segmentInfo.isClosed() && !offloadBuffer.isEmpty())
                // last time to build and upload block
                || bufferLength.get() >= streamingBlockSize
            // buffer size full, build and upload block
        ) {
            List<Entry> entries = new LinkedList<>();
            int blockEntrySize = 0;
            final Entry firstEntry = offloadBuffer.poll();
            entries.add(firstEntry);
            long blockLedgerId = firstEntry.getLedgerId();
            long blockEntryId = firstEntry.getEntryId();

            while (!offloadBuffer.isEmpty() && offloadBuffer.peek().getLedgerId() == blockLedgerId
                    && blockEntrySize <= streamingBlockSize) {
                final Entry entryInBlock = offloadBuffer.poll();
                final int entrySize = entryInBlock.getLength();
                bufferLength.addAndGet(-entrySize);
                blockEntrySize += entrySize;
                entries.add(entryInBlock);
            }
            final int blockSize = BufferedOffloadStream
                    .calculateBlockSize(streamingBlockSize, entries.size(), blockEntrySize);
            try {
                buildBlockAndUpload(blockSize, entries, blockLedgerId, blockEntryId, partId, streamingParts,
                        streamingIndexBuilder, blobStore);
            } catch (InterruptedException | ExecutionException | IOException e) {
                offloadResult.completeExceptionally(e);
                return;
            }
            streamingOffloadLoop(partId + 1, dataObjectLength + blockSize);
        } else {
            log.debug("not enough data, delay schedule for part: {} length: {}", partId, dataObjectLength);
            scheduler.chooseThread(segmentInfo)
                    .schedule(() -> {
                        streamingOffloadLoop(partId, dataObjectLength);
                    }, 100, TimeUnit.MILLISECONDS);
        }
    }

    private void buildBlockAndUpload(int blockSize, List<Entry> entries, long blockLedgerId, long beginEntryId,
                                     int partId, List<MultipartPart> parts,
                                     OffloadIndexBlockV2Builder indexBuilder
            , BlobStore blobStore) throws InterruptedException, ExecutionException, IOException {
        try (final BufferedOffloadStream payloadStream = new BufferedOffloadStream(blockSize, entries,
                blockLedgerId, beginEntryId)) {
            log.debug("begin upload payload: {} {}", blockLedgerId, beginEntryId);
            Payload partPayload = Payloads.newInputStreamPayload(payloadStream);
            partPayload.getContentMetadata().setContentType("application/octet-stream");
            parts.add(blobStore.uploadMultipartPart(streamingMpu, partId, partPayload));
            indexBuilder.withDataBlockHeaderLength(StreamingDataBlockHeaderImpl.getDataStartOffset());
            indexBuilder.addBlock(blockLedgerId, beginEntryId, partId, blockSize);
            final MLDataFormats.ManagedLedgerInfo.LedgerInfo ledgerInfo = ml.getLedgerInfo(blockLedgerId).get();
            final MLDataFormats.ManagedLedgerInfo.LedgerInfo.Builder ledgerInfoBuilder = MLDataFormats.ManagedLedgerInfo.LedgerInfo
                    .newBuilder();
            if (ledgerInfo != null) {
                ledgerInfoBuilder.mergeFrom(ledgerInfo);
            }
            if (ledgerInfoBuilder.getEntries() == 0) {
                //ledger unclosed, use last entry id of the block
                ledgerInfoBuilder.setEntries(payloadStream.getEndEntryId() + 1);
            }
            indexBuilder.addLedgerMeta(blockLedgerId, ledgerInfoBuilder.build());
            log.debug("UploadMultipartPart. container: {}, blobName: {}, partId: {}, mpu: {}",
                    config.getBucket(), streamingDataBlockKey, partId, streamingMpu.id());
        } catch (Throwable e) {
            blobStore.abortMultipartUpload(streamingMpu);
            throw e;
        }
    }

    private void buildIndexAndCompleteResult(long dataObjectLength, BlobStore blobStore,
                                             OffloadIndexBlockV2Builder indexBuilder,
                                             MultipartUpload mpu, List<MultipartPart> mpParts,
                                             String indexKey) throws IOException {
        try {
            blobStore.completeMultipartUpload(mpu, mpParts);
            indexBuilder.withDataObjectLength(dataObjectLength);
            final OffloadIndexBlockV2 index = indexBuilder.buildV2();
            final IndexInputStream indexStream = index.toStream();
            final BlobBuilder indexBlobBuilder = blobStore.blobBuilder(indexKey);
            indexBuilder.withDataBlockHeaderLength(StreamingDataBlockHeaderImpl.getDataStartOffset());

            DataBlockUtils.addVersionInfo(indexBlobBuilder, userMetadata);
            try (final InputStreamPayload indexPayLoad = Payloads.newInputStreamPayload(indexStream)) {
                indexPayLoad.getContentMetadata().setContentLength(indexStream.getStreamSize());
                indexPayLoad.getContentMetadata().setContentType("application/octet-stream");
                final Blob indexBlob = indexBlobBuilder.payload(indexPayLoad)
                        .contentLength(indexStream.getStreamSize())
                        .build();
                blobStore.putBlob(config.getBucket(), indexBlob);
            }
        } catch (Exception e) {
            log.error("streaming offload failed", e);
            throw e;
        }
    }

    private CompletableFuture<OffloadResult> getOffloadResultAsync() {
        return this.offloadResult;
    }

    private synchronized OfferEntryResult offerEntry(Entry entry) {

        if (segmentInfo.isClosed()) {
            log.debug("Segment already closed {}", segmentInfo);
            return OfferEntryResult.FAIL_SEGMENT_CLOSED;
        } else if (maxBufferLength <= bufferLength.get()) {
            //buffer length can over fill maxBufferLength a bit with the last entry
            //to prevent insufficient content to build a block
            return OfferEntryResult.FAIL_BUFFER_FULL;
        } else {
            final EntryImpl entryImpl = EntryImpl
                    .create(entry.getLedgerId(), entry.getEntryId(), entry.getDataBuffer());
            offloadBuffer.add(entryImpl);
            bufferLength.getAndAdd(entryImpl.getLength());
            segmentLength.getAndAdd(entryImpl.getLength());
            lastOfferedPosition = entryImpl.getPosition();
            if (segmentLength.get() >= maxSegmentLength
                    && System.currentTimeMillis() - segmentBeginTimeMillis >= minSegmentCloseTimeMillis) {
                closeSegment();
            }
            return OfferEntryResult.SUCCESS;
        }
    }

    private synchronized boolean closeSegment() {
        final boolean result = !segmentInfo.isClosed();
        log.debug("close segment {} {}", lastOfferedPosition.getLedgerId(), lastOfferedPosition.getEntryId());
        this.segmentInfo.closeSegment(lastOfferedPosition.getLedgerId(), lastOfferedPosition.getEntryId());
        return result;
    }

    private PositionImpl lastOffered() {
        return lastOfferedPosition;
    }

    /**
     * Attempts to create a BlobStoreLocation from the values in the offloadDriverMetadata,
     * however, if no values are available, it defaults to the currently configured
     * provider, region, bucket, etc.
     *
     * @param offloadDriverMetadata
     * @return
     */
    private BlobStoreLocation getBlobStoreLocation(Map<String, String> offloadDriverMetadata) {
        return (!offloadDriverMetadata.isEmpty()) ? new BlobStoreLocation(offloadDriverMetadata) :
            new BlobStoreLocation(getOffloadDriverMetadata());
    }

    @Override
    public CompletableFuture<ReadHandle> readOffloaded(long ledgerId, UUID uid,
                                                       Map<String, String> offloadDriverMetadata) {

        BlobStoreLocation bsKey = getBlobStoreLocation(offloadDriverMetadata);
        String readBucket = bsKey.getBucket();
        BlobStore readBlobstore = blobStores.get(config.getBlobStoreLocation());

        CompletableFuture<ReadHandle> promise = new CompletableFuture<>();
        String key = DataBlockUtils.dataBlockOffloadKey(ledgerId, uid);
        String indexKey = DataBlockUtils.indexBlockOffloadKey(ledgerId, uid);
        scheduler.chooseThread(ledgerId).submit(() -> {
            try {
                promise.complete(BlobStoreBackedReadHandleImpl.open(scheduler.chooseThread(ledgerId),
                        readBlobstore,
                        readBucket, key, indexKey,
                        DataBlockUtils.VERSION_CHECK,
                        ledgerId, config.getReadBufferSizeInBytes()));
            } catch (Throwable t) {
                log.error("Failed readOffloaded: ", t);
                promise.completeExceptionally(t);
            }
        });
        return promise;
    }

    @Override
    public CompletableFuture<ReadHandle> readOffloaded(long ledgerId, MLDataFormats.OffloadContext ledgerContext,
                                                       Map<String, String> offloadDriverMetadata) {
        BlobStoreLocation bsKey = getBlobStoreLocation(offloadDriverMetadata);
        String readBucket = bsKey.getBucket();
        BlobStore readBlobstore = blobStores.get(config.getBlobStoreLocation());
        CompletableFuture<ReadHandle> promise = new CompletableFuture<>();
        final List<MLDataFormats.OffloadSegment> offloadSegmentList = ledgerContext.getOffloadSegmentList();
        List<String> keys = Lists.newLinkedList();
        List<String> indexKeys = Lists.newLinkedList();
        offloadSegmentList.forEach(seg -> {
            final UUID uuid = new UUID(seg.getUidMsb(), seg.getUidLsb());
            final String key = uuid.toString();
            final String indexKey = DataBlockUtils.indexBlockOffloadKey(uuid);
            keys.add(key);
            indexKeys.add(indexKey);
        });

        scheduler.chooseThread(ledgerId).submit(() -> {
            try {
                promise.complete(BlobStoreBackedReadHandleImplV2.open(scheduler.chooseThread(ledgerId),
                        readBlobstore,
                        readBucket, keys, indexKeys,
                        DataBlockUtils.VERSION_CHECK,
                        ledgerId, config.getReadBufferSizeInBytes()));
            } catch (Throwable t) {
                log.error("Failed readOffloaded: ", t);
                promise.completeExceptionally(t);
            }
        });
        return promise;
    }

    @Override
    public CompletableFuture<ReadHandle> readOffloaded(String managedLedgerName, long ledgerId,
                                                       Map<String, String> offloadDriverMetadata) {
        final BlobStoreLocation bsKey = getBlobStoreLocation(offloadDriverMetadata);
        final String readBucket = bsKey.getBucket();
        final BlobStore readBlobStore = blobStores.get(config.getBlobStoreLocation());
        CompletableFuture<ReadHandle> promise = new CompletableFuture<>();
        final ArrayList<String> keys = Lists.newArrayList(ledgerOffloadKey(managedLedgerName, ledgerId));
        final ArrayList<String> indexKeys = Lists.newArrayList(ledgerOffloadIndexKey(managedLedgerName, ledgerId));
        scheduler.chooseThread(ledgerId).submit(() -> {
            try {
                promise.complete(BlobStoreBackedReadHandleImplV2.open(scheduler.chooseThread(ledgerId),
                        readBlobStore,
                        readBucket, keys, indexKeys,
                        DataBlockUtils.VERSION_CHECK,
                        ledgerId, config.getReadBufferSizeInBytes()));
            } catch (Throwable t) {
                log.error("Failed readOffloaded: ", t);
                promise.completeExceptionally(t);
            }
        });
        return promise;
    }

    @Override
    public CompletableFuture<ReadHandle> deleteOffloaded(String managedLedgerName, long ledgerId,
                                                         Map<String, String> offloadDriverMetadata) {
        final BlobStoreLocation bsKey = getBlobStoreLocation(offloadDriverMetadata);
        final String readBucket = bsKey.getBucket();
        final BlobStore blobStore = blobStores.get(config.getBlobStoreLocation());
        CompletableFuture<ReadHandle> promise = new CompletableFuture<>();

        scheduler.chooseThread(ledgerId).submit(() -> {
            try {
                blobStore.removeBlobs(readBucket,
                        ImmutableList.of(ledgerOffloadKey(managedLedgerName, ledgerId),
                                ledgerOffloadIndexKey(managedLedgerName, ledgerId)));
                promise.complete(null);
            } catch (Throwable t) {
                log.error("Failed delete Blob", t);
                promise.completeExceptionally(t);
            }
        });

        return promise;
    }

    @Override
    public CompletableFuture<Void> deleteOffloaded(long ledgerId, UUID uid,
                                                   Map<String, String> offloadDriverMetadata) {
        BlobStoreLocation bsKey = getBlobStoreLocation(offloadDriverMetadata);
        String readBucket = bsKey.getBucket(offloadDriverMetadata);
        BlobStore readBlobstore = blobStores.get(config.getBlobStoreLocation());

        CompletableFuture<Void> promise = new CompletableFuture<>();
        scheduler.chooseThread(ledgerId).submit(() -> {
            try {
                readBlobstore.removeBlobs(readBucket,
                        ImmutableList.of(DataBlockUtils.dataBlockOffloadKey(ledgerId, uid),
                                DataBlockUtils.indexBlockOffloadKey(ledgerId, uid)));
                promise.complete(null);
            } catch (Throwable t) {
                log.error("Failed delete Blob", t);
                promise.completeExceptionally(t);
            }
        });

        return promise;
    }

    @Override
    public CompletableFuture<Void> deleteOffloaded(UUID uid, Map<String, String> offloadDriverMetadata) {
        BlobStoreLocation bsKey = getBlobStoreLocation(offloadDriverMetadata);
        String readBucket = bsKey.getBucket(offloadDriverMetadata);
        BlobStore readBlobstore = blobStores.get(config.getBlobStoreLocation());

        CompletableFuture<Void> promise = new CompletableFuture<>();
        scheduler.submit(() -> {
            try {
                readBlobstore.removeBlobs(readBucket,
                        ImmutableList.of(uid.toString(),
                                DataBlockUtils.indexBlockOffloadKey(uid)));
                promise.complete(null);
            } catch (Throwable t) {
                log.error("Failed delete Blob", t);
                promise.completeExceptionally(t);
            }
        });

        return promise;
    }

    @Override
    public OffloadPolicies getOffloadPolicies() {
        Properties properties = new Properties();
        properties.putAll(config.getConfigProperties());
        return OffloadPolicies.create(properties);
    }

    @Override
    public void close() {
        for (BlobStore readBlobStore : blobStores.values()) {
            if (readBlobStore != null) {
                readBlobStore.getContext().close();
            }
        }
    }
}
