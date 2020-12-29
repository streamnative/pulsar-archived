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

import static com.google.common.base.Preconditions.checkState;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
import org.apache.bookkeeper.client.api.LedgerMetadata;
import org.apache.bookkeeper.mledger.offload.jcloud.StreamingOffloadIndexBlock;
import org.apache.bookkeeper.mledger.offload.jcloud.StreamingOffloadIndexBlockBuilder;

/**
 * Interface for builder of index block used for offload a ledger to long term storage.
 */
public class StreamingOffloadIndexBlockBuilderImpl implements StreamingOffloadIndexBlockBuilder {

    private final Map<Long, LedgerMetadata> ledgerMetadata = new HashMap<>();
    private long dataObjectLength;
    private long dataHeaderLength;
    private final SortedMap<Long, List<OffloadIndexEntryImpl>> entryMap;
    private long offset = 0;
    private int lastBlockSize;

    public StreamingOffloadIndexBlockBuilderImpl() {
        this.entryMap = new TreeMap<>();
    }

    @Override
    public StreamingOffloadIndexBlockBuilder withDataObjectLength(long dataObjectLength) {
        this.dataObjectLength = dataObjectLength;
        return this;
    }

    @Override
    public StreamingOffloadIndexBlockBuilder withDataBlockHeaderLength(long dataHeaderLength) {
        this.dataHeaderLength = dataHeaderLength;
        return this;
    }

    @Override
    public StreamingOffloadIndexBlockBuilder addLedgerMeta(Long ledgerId, LedgerMetadata metadata) {
        this.ledgerMetadata.put(ledgerId, metadata);
        return this;
    }

    @Override
    public StreamingOffloadIndexBlockBuilder addBlock(long ledgerId, long firstEntryId, int partId, int blockSize) {
        checkState(dataHeaderLength > 0);

        offset = offset + lastBlockSize;
        lastBlockSize = blockSize;

        entryMap.getOrDefault(ledgerId, new LinkedList<>())
                .add(OffloadIndexEntryImpl.of(firstEntryId, partId, offset, dataHeaderLength));
        return this;
    }

    @Override
    public StreamingOffloadIndexBlock fromStream(InputStream is) throws IOException {
        return StreamingOffloadIndexBlockImpl.get(is);
    }

    @Override
    public StreamingOffloadIndexBlock build() {
        checkState(ledgerMetadata != null);
        checkState(!entryMap.isEmpty());
        checkState(dataObjectLength > 0);
        checkState(dataHeaderLength > 0);
        return StreamingOffloadIndexBlockImpl.get(ledgerMetadata, dataObjectLength, dataHeaderLength, entryMap);
    }

}
