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
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicLong;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.client.api.ReadHandle;
import org.apache.bookkeeper.common.util.OrderedScheduler;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.LedgerOffloader;
import org.apache.bookkeeper.mledger.ManagedLedger;
import org.apache.bookkeeper.mledger.impl.EntryImpl;
import org.apache.bookkeeper.mledger.impl.ManagedLedgerImpl;
import org.apache.bookkeeper.mledger.impl.PositionImpl;
import org.apache.bookkeeper.mledger.offload.jcloud.BlockAwareSegmentInputStream;
import org.apache.bookkeeper.mledger.offload.jcloud.OffloadIndexBlock;
import org.apache.bookkeeper.mledger.offload.jcloud.OffloadIndexBlockBuilder;
import org.apache.bookkeeper.mledger.offload.jcloud.StreamingOffloadIndexBlock;
import org.apache.bookkeeper.mledger.offload.jcloud.StreamingOffloadIndexBlockBuilder;
import org.apache.bookkeeper.mledger.offload.jcloud.provider.BlobStoreLocation;
import org.apache.bookkeeper.mledger.offload.jcloud.provider.TieredStorageConfiguration;
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
    //TODO start offloading
    //TODO read offloaded
    //TODO delete offloaded
    //TODO buffer should not less than max message size
    //TODO need padding magic
    //TODO create new block when ending a ledger
    //TODO change offer result when segment closed
    //TODO add meta info for every ledger in index builder

    private final OrderedScheduler scheduler;
    private final TieredStorageConfiguration config;
    private final Location writeLocation;

    // metadata to be stored as part of the offloaded ledger metadata
    private final Map<String, String> userMetadata;

    private final ConcurrentMap<BlobStoreLocation, BlobStore> blobStores = new ConcurrentHashMap<>();
    private SegmentInfo segmentInfo;
    private AtomicLong bufferLength = new AtomicLong(0);
    final private long maxBufferLength = 10 * 1024 * 1024; //TODO initialize by configuration
    final private ConcurrentLinkedQueue<Entry> offloadBuffer = new ConcurrentLinkedQueue<>();
    private CompletableFuture<OffloadResult> offloadResult;
    private volatile PositionImpl lastOfferedPosition = PositionImpl.latest;
    private Instant segmentCloseTime = Instant.now().plusSeconds(600); //TODO initialize by configuration
    private long maxSegmentLength = 1024 * 1024 * 1024; //TODO initialize by configuration
    private int streamingBlockSize = 64 * 1024 * 1024; //TODO initialize by configuration
    private AtomicLong writtenLength = new AtomicLong(0);
    private ManagedLedgerImpl ml;
    private StreamingOffloadIndexBlockBuilder streamingIndexBuilder;

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
                 OffloadIndexBlock.IndexInputStream indexStream = index.toStream()) {
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

    BlobStore blobStore;
    String streamingDataBlockKey;
    String streamingDataIndexKey;
    MultipartUpload streamingMpu = null;
    List<MultipartPart> streamingParts = Lists.newArrayList();

    @Override
    public CompletableFuture<OffloaderHandle> streamingOffload(ManagedLedger ml, UUID uuid, long beginLedger,
                                                               long beginEntry,
                                                               Map<String, String> driverMetadata) {
        this.ml = (ManagedLedgerImpl) ml;
        this.segmentInfo = new SegmentInfo(uuid, beginLedger, beginEntry, config.getDriver(), driverMetadata);
        this.offloadResult = new CompletableFuture<>();
        blobStore = blobStores.get(config.getBlobStoreLocation());
        streamingIndexBuilder = new StreamingOffloadIndexBlockBuilderImpl();
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

        return CompletableFuture.completedFuture(new OffloaderHandle() {
            @Override
            public boolean canOffer(long size) {
                return BlobStoreManagedLedgerOffloader.this.canOffer(size);
            }

            @Override
            public PositionImpl lastOffered() {
                return BlobStoreManagedLedgerOffloader.this.lastOffered();
            }

            @Override
            public boolean offerEntry(EntryImpl entry) {
                return BlobStoreManagedLedgerOffloader.this.offerEntry(entry);
            }

            @Override
            public CompletableFuture<OffloadResult> getOffloadResultAsync() {
                return BlobStoreManagedLedgerOffloader.this.getOffloadResultAsync();
            }
        });
    }

    private void streamingOffloadLoop(int partId, int dataObjectLength) {
        if (segmentInfo.isClosed() && offloadBuffer.isEmpty()) {
            offloadResult.complete(new OffloadResult());
            return;
        }
        final BufferedOffloadStream payloadStream;

        while (offloadBuffer.isEmpty()) {
            if (segmentInfo.isClosed()) {
                offloadResult.complete(new OffloadResult());
            } else {
                try {
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
        final Entry peek = offloadBuffer.peek();
        //initialize payload when there is at least one entry
        final long blockLedgerId = peek.getLedgerId();
        final long blockEntryId = peek.getEntryId();
        payloadStream = new BufferedOffloadStream(streamingBlockSize, offloadBuffer, segmentInfo,
                blockLedgerId, blockEntryId, bufferLength);

        try {
            streamingIndexBuilder.addLedgerMeta(blockLedgerId, ml.getRawLedgerMetadata(blockLedgerId).get());
            streamingIndexBuilder.withDataBlockHeaderLength(StreamingDataBlockHeaderImpl.getDataStartOffset());
        } catch (InterruptedException | ExecutionException e) {
            offloadResult.completeExceptionally(e);
        }

        Payload partPayload = Payloads.newInputStreamPayload(payloadStream);
        partPayload.getContentMetadata().setContentType("application/octet-stream");
        streamingParts.add(blobStore.uploadMultipartPart(streamingMpu, partId, partPayload));
        streamingIndexBuilder.addBlock(blockLedgerId, blockEntryId, partId, streamingBlockSize);

        log.debug("UploadMultipartPart. container: {}, blobName: {}, partId: {}, mpu: {}",
                config.getBucket(), streamingDataBlockKey, partId, streamingMpu.id());
        if (segmentInfo.isClosed() && offloadBuffer.isEmpty()) {
            try {
                blobStore.completeMultipartUpload(streamingMpu, streamingParts);
                streamingIndexBuilder.withDataObjectLength(dataObjectLength + streamingBlockSize);
                final StreamingOffloadIndexBlock index = streamingIndexBuilder.build();
                final StreamingOffloadIndexBlock.IndexInputStream indexStream = index.toStream();
                final BlobBuilder indexBlobBuilder = blobStore.blobBuilder(streamingDataIndexKey);
                final InputStreamPayload indexPayLoad = Payloads.newInputStreamPayload(indexStream);
                indexPayLoad.getContentMetadata().setContentLength(indexStream.getStreamSize());
                indexPayLoad.getContentMetadata().setContentType("application/octet-stream");
                final Blob indexBlob = indexBlobBuilder.payload(indexPayLoad)
                        .contentLength(indexStream.getStreamSize())
                        .build();
                blobStore.putBlob(config.getBucket(), indexBlob);

                offloadResult.complete(new OffloadResult());
            } catch (Exception e) {
                log.error("streaming offload failed", e);
                offloadResult.completeExceptionally(e);
            }
        } else {
            scheduler.chooseThread(segmentInfo)
                    .execute(() -> streamingOffloadLoop(partId + 1, dataObjectLength + streamingBlockSize));
        }
    }

    private CompletableFuture<OffloadResult> getOffloadResultAsync() {
        return this.offloadResult;
    }

    private boolean offerEntry(EntryImpl entry) {
        if (segmentInfo.isClosed()) {
            //TODO return enum to deal with segment closed
            return false;
        } else {
            entry.retain();
            offloadBuffer.add(entry);
            bufferLength.getAndAdd(entry.getLength());
            return true;
        }
    }

    private PositionImpl lastOffered() {
        return lastOfferedPosition;
    }

    private boolean canOffer(long size) {
        return maxBufferLength >= bufferLength.get() + size;
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
