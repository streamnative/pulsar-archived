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

import static org.apache.bookkeeper.mledger.offload.OffloadUtils.buildLedgerMetadataFormat;
import com.google.common.collect.Maps;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufInputStream;
import io.netty.util.Recycler;
import io.netty.util.Recycler.Handle;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;
import org.apache.bookkeeper.client.api.DigestType;
import org.apache.bookkeeper.client.api.LedgerMetadata;
import org.apache.bookkeeper.mledger.offload.jcloud.OffloadIndexEntry;
import org.apache.bookkeeper.mledger.offload.jcloud.StreamingOffloadIndexBlock;
import org.apache.bookkeeper.net.BookieId;
import org.apache.bookkeeper.proto.DataFormats;
import org.apache.bookkeeper.proto.DataFormats.LedgerMetadataFormat;
import org.apache.pulsar.common.allocator.PulsarByteBufAllocator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StreamingOffloadIndexBlockImpl implements StreamingOffloadIndexBlock {
    private static final Logger log = LoggerFactory.getLogger(OffloadIndexBlockImpl.class);

    private static final int INDEX_MAGIC_WORD = 0x3D1FB0BC;

    private Map<Long, LedgerMetadata> segmentMetadata;
    private long dataObjectLength;
    private long dataHeaderLength;
    //    private TreeMap<Long, OffloadIndexEntryImpl> indexEntries;
    private Map<Long, TreeMap<Long, OffloadIndexEntryImpl>> indexEntries;


    private final Handle<StreamingOffloadIndexBlockImpl> recyclerHandle;

    private static final Recycler<StreamingOffloadIndexBlockImpl> RECYCLER = new Recycler<StreamingOffloadIndexBlockImpl>() {
        @Override
        protected StreamingOffloadIndexBlockImpl newObject(Recycler.Handle<StreamingOffloadIndexBlockImpl> handle) {
            return new StreamingOffloadIndexBlockImpl(handle);
        }
    };

    private StreamingOffloadIndexBlockImpl(Handle<StreamingOffloadIndexBlockImpl> recyclerHandle) {
        this.recyclerHandle = recyclerHandle;
    }

    public static StreamingOffloadIndexBlockImpl get(Map<Long, LedgerMetadata> metadata, long dataObjectLength,
                                                     long dataHeaderLength,
                                                     Map<Long, List<OffloadIndexEntryImpl>> entries) {
        StreamingOffloadIndexBlockImpl block = RECYCLER.get();
        block.indexEntries = new HashMap<>();
        entries.forEach((ledgerId, list) -> {
            final TreeMap<Long, OffloadIndexEntryImpl> inLedger = block.indexEntries
                    .getOrDefault(ledgerId, new TreeMap<>());
            list.forEach(indexEntry -> {
                inLedger.put(indexEntry.getFirstEntryId(), indexEntry);
            });
            block.indexEntries.put(ledgerId, inLedger);
        });

        block.segmentMetadata = metadata;
        block.dataObjectLength = dataObjectLength;
        block.dataHeaderLength = dataHeaderLength;
        return block;
    }

    public static StreamingOffloadIndexBlockImpl get(InputStream stream) throws IOException {
        StreamingOffloadIndexBlockImpl block = RECYCLER.get();
        block.indexEntries = Maps.newTreeMap();
        block.fromStream(stream);
        return block;
    }

    public void recycle() {
        dataObjectLength = -1;
        dataHeaderLength = -1;
        segmentMetadata = null;
        indexEntries.clear();
        indexEntries = null;
        if (recyclerHandle != null) {
            recyclerHandle.recycle(this);
        }
    }

    @Override
    public OffloadIndexEntry getIndexEntryForEntry(long ledgerId, long messageEntryId) throws IOException {
        //TODO deal with data exists in another object
        if (messageEntryId > segmentMetadata.get(ledgerId).getLastEntryId()) {
            log.warn("Try to get entry: {}, which beyond lastEntryId {}, return null",
                    messageEntryId, segmentMetadata.get(ledgerId).getLastEntryId());
            throw new IndexOutOfBoundsException("Entry index: " + messageEntryId
                    + " beyond lastEntryId: " + segmentMetadata.get(ledgerId).getLastEntryId());
        }
        // find the greatest mapping Id whose entryId <= messageEntryId
        return this.indexEntries.get(ledgerId).floorEntry(messageEntryId).getValue();
    }

    @Override
    public int getEntryCount() {
        return this.indexEntries.size();
    }

    @Override
    public Map<Long, LedgerMetadata> getLedgerMetadata() {
        return this.segmentMetadata;
    }

    @Override
    public long getDataObjectLength() {
        return this.dataObjectLength;
    }

    @Override
    public long getDataBlockHeaderLength() {
        return this.dataHeaderLength;
    }

    /**
     * Get the content of the index block as InputStream.
     * Read out in format:
     *   | index_magic_header | index_block_len | data_object_len | data_header_len |
     *   | index_entry_count  | segment_metadata_len | segment metadata | index entries... |
     */
    @Override
    public StreamingOffloadIndexBlock.IndexInputStream toStream() throws IOException {
        int indexEntryCount = this.indexEntries.size();
        byte[] ledgerMetadataByte = buildLedgerMetadataFormat(this.segmentMetadata);
        int segmentMetadataLength = ledgerMetadataByte.length;

        int indexBlockLength = 4 /* magic header */
                + 4 /* index block length */
                + 8 /* data object length */
                + 8 /* data header length */
                + 4 /* index entry count */
                + 4 /* segment metadata length */
                + segmentMetadataLength
                + indexEntryCount * (8 + 4 + 8); /* messageEntryId + blockPartId + blockOffset */

        ByteBuf out = PulsarByteBufAllocator.DEFAULT.buffer(indexBlockLength, indexBlockLength);

        out.writeInt(INDEX_MAGIC_WORD)
                .writeInt(indexBlockLength)
                .writeLong(dataObjectLength)
                .writeLong(dataHeaderLength)
                .writeInt(indexEntryCount)
                .writeInt(segmentMetadataLength);
        // write metadata
        out.writeBytes(ledgerMetadataByte);

        // write entries
        this.indexEntries.entrySet().forEach(entry ->
                out.writeLong(entry.getValue().getFirstEntryId())
                        .writeInt(entry.getValue().getPartId())
                        .writeLong(entry.getValue().getOffset()));

        return new StreamingOffloadIndexBlock.IndexInputStream(new ByteBufInputStream(out, true), indexBlockLength);
    }

    static private class InternalLedgerMetadata implements LedgerMetadata {

        private int ensembleSize;
        private int writeQuorumSize;
        private int ackQuorumSize;
        private long lastEntryId;
        private long length;
        private DataFormats.LedgerMetadataFormat.DigestType digestType;
        private long ctime;
        private State state;
        private Map<String, byte[]> customMetadata = Maps.newHashMap();
        private TreeMap<Long, ArrayList<BookieId>> ensembles =
                new TreeMap<>();

        InternalLedgerMetadata(LedgerMetadataFormat ledgerMetadataFormat) {
            this.ensembleSize = ledgerMetadataFormat.getEnsembleSize();
            this.writeQuorumSize = ledgerMetadataFormat.getQuorumSize();
            this.ackQuorumSize = ledgerMetadataFormat.getAckQuorumSize();
            this.lastEntryId = ledgerMetadataFormat.getLastEntryId();
            this.length = ledgerMetadataFormat.getLength();
            this.digestType = ledgerMetadataFormat.getDigestType();
            this.ctime = ledgerMetadataFormat.getCtime();
            this.state = org.apache.bookkeeper.client.api.LedgerMetadata.State.valueOf(
                    ledgerMetadataFormat.getState().toString());

            if (ledgerMetadataFormat.getCustomMetadataCount() > 0) {
                ledgerMetadataFormat.getCustomMetadataList().forEach(
                        entry -> this.customMetadata.put(entry.getKey(), entry.getValue().toByteArray()));
            }

            ledgerMetadataFormat.getSegmentList().forEach(segment -> {
                ArrayList<BookieId> addressArrayList = new ArrayList<>();
                segment.getEnsembleMemberList().forEach(address -> {
                    try {
                        addressArrayList.add(BookieId.parse(address));
                    } catch (IllegalArgumentException e) {
                        log.error("Exception when create BookieSocketAddress. ", e);
                    }
                });
                this.ensembles.put(segment.getFirstEntryId(), addressArrayList);
            });
        }

        @Override
        public long getLedgerId() {
            throw new UnsupportedOperationException();
        }

        @Override
        public int getEnsembleSize() {
            return this.ensembleSize;
        }

        @Override
        public int getWriteQuorumSize() {
            return this.writeQuorumSize;
        }

        @Override
        public int getAckQuorumSize() {
            return this.ackQuorumSize;
        }

        @Override
        public long getLastEntryId() {
            return this.lastEntryId;
        }

        @Override
        public long getLength() {
            return this.length;
        }

        @Override
        public DigestType getDigestType() {
            switch (this.digestType) {
                case HMAC:
                    return DigestType.MAC;
                case CRC32:
                    return DigestType.CRC32;
                case CRC32C:
                    return DigestType.CRC32C;
                case DUMMY:
                    return DigestType.DUMMY;
                default:
                    throw new IllegalArgumentException("Unable to convert digest type " + digestType);
            }
        }

        @Override
        public long getCtime() {
            return this.ctime;
        }

        @Override
        public boolean isClosed() {
            return this.state == State.CLOSED;
        }

        @Override
        public Map<String, byte[]> getCustomMetadata() {
            return this.customMetadata;
        }

        @Override
        public List<BookieId> getEnsembleAt(long entryId) {
            return ensembles.get(ensembles.headMap(entryId + 1).lastKey());
        }

        @Override
        public NavigableMap<Long, ? extends List<BookieId>> getAllEnsembles() {
            return this.ensembles;
        }

        @Override
        public long getCToken() {
            // TODO Auto-generated method stub
            return 0;
        }

        @Override
        public int getMetadataFormatVersion() {
            // TODO Auto-generated method stub
            return 0;
        }

        @Override
        public byte[] getPassword() {
            // TODO Auto-generated method stub
            return null;
        }

        @Override
        public State getState() {
            return this.state;
        }

        @Override
        public boolean hasPassword() {
            // TODO Auto-generated method stub
            return false;
        }

        @Override
        public String toSafeString() {
            // TODO Auto-generated method stub
            return null;
        }
    }

    private static LedgerMetadata parseLedgerMetadata(byte[] bytes) throws IOException {
        LedgerMetadataFormat.Builder builder = LedgerMetadataFormat.newBuilder();
        builder.mergeFrom(bytes);
        return new InternalLedgerMetadata(builder.build());
    }

    private StreamingOffloadIndexBlock fromStream(InputStream stream) throws IOException {
        DataInputStream dis = new DataInputStream(stream);
        int magic = dis.readInt();
        if (magic != this.INDEX_MAGIC_WORD) {
            throw new IOException(String.format("Invalid MagicWord. read: 0x%x  expected: 0x%x",
                    magic, INDEX_MAGIC_WORD));
        }
        dis.readInt(); // no used index block length
        this.dataObjectLength = dis.readLong();
        this.dataHeaderLength = dis.readLong();
        int indexEntryCount = dis.readInt();
        int segmentMetadataLength = dis.readInt();

        byte[] metadataBytes = new byte[segmentMetadataLength];

        if (segmentMetadataLength != dis.read(metadataBytes)) {
            log.error("Read ledgerMetadata from bytes failed");
            throw new IOException("Read ledgerMetadata from bytes failed");
        }
        this.segmentMetadata = parseLedgerMetadata(metadataBytes);

        for (int i = 0; i < indexEntryCount; i++) {
            long entryId = dis.readLong();
            this.indexEntries.putIfAbsent(entryId, OffloadIndexEntryImpl.of(entryId, dis.readInt(),
                    dis.readLong(), dataHeaderLength));
        }

        return this;
    }

    public static int getIndexMagicWord() {
        return INDEX_MAGIC_WORD;
    }

    @Override
    public void close() {
        recycle();
    }

}

