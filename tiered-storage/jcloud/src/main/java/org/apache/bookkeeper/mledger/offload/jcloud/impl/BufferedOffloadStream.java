package org.apache.bookkeeper.mledger.offload.jcloud.impl;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.CompositeByteBuf;
import java.io.IOException;
import java.io.InputStream;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicLong;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.LedgerOffloader.SegmentInfo;
import org.apache.pulsar.common.allocator.PulsarByteBufAllocator;

@Slf4j
public class BufferedOffloadStream extends InputStream {
    private static final int[] BLOCK_END_PADDING = BlockAwareSegmentInputStreamImpl.BLOCK_END_PADDING;
    private final SegmentInfo segmentInfo;
    private AtomicLong bufferLength;
    static final int ENTRY_HEADER_SIZE = 4 /* entry size */ + 8 /* entry id */;

    private final long blockSize;
    private final ConcurrentLinkedQueue<Entry> entryBuffer;
    private final InputStream blockHead;
    int offset = 0;
    static int NOT_INITIALIZED = -1;
    int validDataOffset = NOT_INITIALIZED;
    CompositeByteBuf currentEntry;

    public BufferedOffloadStream(int blockSize,
                                 ConcurrentLinkedQueue<Entry> entryBuffer,
                                 SegmentInfo segmentInfo,
                                 long beginLedgerId,
                                 long beginEntryId,
                                 AtomicLong bufferLength) {
        this.blockSize = blockSize;
        this.segmentInfo = segmentInfo;
        this.entryBuffer = entryBuffer;
        this.bufferLength = bufferLength;
        this.blockHead = StreamingDataBlockHeaderImpl.of(blockSize, beginLedgerId, beginEntryId)
                .toStream();
    }


    @Override
    public int read() throws IOException {
        if (blockHead.available() > 0) {
            offset++;
            return blockHead.read();
        }
        //if current exists, use current first
        if (currentEntry != null) {
            if (currentEntry.readableBytes() > 0) {
                offset += 1;
                return currentEntry.readUnsignedByte();
            } else {
                currentEntry.release();
                currentEntry = null;
            }
        }

        if (blockSize == offset) {
            return -1;
        } else if (validDataOffset != NOT_INITIALIZED) {
            return BLOCK_END_PADDING[offset++ - validDataOffset];
        }

        Entry headEntry;

        while ((headEntry = entryBuffer.peek()) == null) {
            if (segmentInfo.isClosed()) {
                if (validDataOffset == NOT_INITIALIZED) {
                    validDataOffset = offset;
                }
                return read();
            } else {
                try {
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                    log.error("sleep failed", e);
                }
            }
        }

        if (blockSize >= offset
                + ENTRY_HEADER_SIZE
                + headEntry.getLength()) {
            entryBuffer.poll();
            final int entryLength = headEntry.getLength();
            bufferLength.getAndAdd(-entryLength);
            final long entryId = headEntry.getEntryId();
            CompositeByteBuf entryBuf = PulsarByteBufAllocator.DEFAULT.compositeBuffer(2);
            ByteBuf entryHeaderBuf = PulsarByteBufAllocator.DEFAULT.buffer(ENTRY_HEADER_SIZE, ENTRY_HEADER_SIZE);
            entryHeaderBuf.writeInt(entryLength).writeLong(entryId);
            entryBuf.addComponents(true, entryHeaderBuf, headEntry.getDataBuffer());
            currentEntry = entryBuf;

            return read();
        } else {
            //over sized, fill padding
            if (validDataOffset == NOT_INITIALIZED) {
                validDataOffset = offset;
            }
            return BLOCK_END_PADDING[offset++ - validDataOffset];
        }
    }
}
