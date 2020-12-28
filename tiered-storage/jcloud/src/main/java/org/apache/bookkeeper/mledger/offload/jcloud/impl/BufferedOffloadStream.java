package org.apache.bookkeeper.mledger.offload.jcloud.impl;

import java.io.IOException;
import java.io.InputStream;
import java.util.concurrent.ConcurrentLinkedQueue;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.LedgerOffloader.SegmentInfo;

@Slf4j
public class BufferedOffloadStream extends InputStream {
    private static final int[] BLOCK_END_PADDING = BlockAwareSegmentInputStreamImpl.BLOCK_END_PADDING;
    private final SegmentInfo segmentInfo;

    private final long blockSize;
    private final ConcurrentLinkedQueue<Entry> entryBuffer;
    private final InputStream blockHead;
    int offset = 0;
    static int NOT_INITIALIZED = -1;
    int validDataOffset = NOT_INITIALIZED;
    Entry currentEntry;

    public BufferedOffloadStream(int blockSize,
                                 ConcurrentLinkedQueue<Entry> entryBuffer,
                                 SegmentInfo segmentInfo) {
        this.blockSize = blockSize;
        this.entryBuffer = entryBuffer;
        this.segmentInfo = segmentInfo;
        this.blockHead = StreamingDataBlockHeaderImpl.of(blockSize, segmentInfo.beginLedger, segmentInfo.beginEntry)
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
            if (currentEntry.getDataBuffer().readableBytes() > 0) {
                offset += 1;
                return currentEntry.getDataBuffer().readUnsignedByte();
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

        Entry head;

        while ((head = entryBuffer.peek()) == null) {
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

        if (blockSize >= offset + head.getLength()) {
            currentEntry = entryBuffer.poll();
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
