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
package org.apache.bookkeeper.mledger.impl;


import com.google.common.base.Predicate;
import io.netty.buffer.ByteBuf;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.mledger.AsyncCallbacks.AddEntryCallback;
import org.apache.bookkeeper.mledger.AsyncCallbacks.CloseCallback;
import org.apache.bookkeeper.mledger.AsyncCallbacks.DeleteCursorCallback;
import org.apache.bookkeeper.mledger.AsyncCallbacks.DeleteLedgerCallback;
import org.apache.bookkeeper.mledger.AsyncCallbacks.OffloadCallback;
import org.apache.bookkeeper.mledger.AsyncCallbacks.OpenCursorCallback;
import org.apache.bookkeeper.mledger.AsyncCallbacks.TerminateCallback;
import org.apache.bookkeeper.mledger.AsyncCallbacks.UpdatePropertiesCallback;
import org.apache.bookkeeper.mledger.Entry;
import org.apache.bookkeeper.mledger.ManagedCursor;
import org.apache.bookkeeper.mledger.ManagedLedger;
import org.apache.bookkeeper.mledger.ManagedLedgerConfig;
import org.apache.bookkeeper.mledger.ManagedLedgerException;
import org.apache.bookkeeper.mledger.ManagedLedgerMXBean;
import org.apache.bookkeeper.mledger.Position;
import org.apache.bookkeeper.mledger.intercept.ManagedLedgerInterceptor;
import org.apache.bookkeeper.mledger.proto.MLDataFormats;
import org.apache.pulsar.common.api.proto.CommandSubscribe.InitialPosition;


@Slf4j
public class TieredManagedLedger implements ManagedLedger {

    private ManagedLedgerConfig originalConfig;
    private final ManagedLedger ledger;
    static private final String offloadCursorName = "_offload_cursor";
    private volatile ManagedCursor offloadCursor;

    TieredManagedLedger(ManagedLedgerConfig originalConfig,
                        ManagedLedger ledger) {
        this.originalConfig = originalConfig;
        this.ledger = ledger;
    }

    @Override
    public synchronized void initialize(ManagedLedgerInitializeLedgerCallback callback, Object ctx) {
        final ManagedLedgerInitializeLedgerCallback wrappedCalllback = new ManagedLedgerInitializeLedgerCallback() {
            @Override
            public void initializeComplete() {
                try {
                    initializeOffloadCursor();
                } catch (ManagedLedgerException | InterruptedException e) {
                    log.error("initialize offload cursor failed", e);
                    callback.initializeFailed(new ManagedLedgerException(e));
                }
                callback.initializeComplete();
            }

            @Override
            public void initializeFailed(ManagedLedgerException e) {
                callback.initializeFailed(e);
            }
        };
        ledger.initialize(wrappedCalllback, ctx);
    }

    public void initializeOffloadCursor() throws ManagedLedgerException, InterruptedException {
        offloadCursor = ledger.openCursor(offloadCursorName, InitialPosition.Earliest);
        offloadCursor.setActive();
    }

    @Override
    public String getName() {
        return ledger.getName();
    }

    @Override
    public Position addEntry(byte[] bytes) throws InterruptedException, ManagedLedgerException {
        return ledger.addEntry(bytes);
    }

    @Override
    public Position addEntry(byte[] data, int numberOfMessages) throws InterruptedException, ManagedLedgerException {
        return ledger.addEntry(data, numberOfMessages);
    }

    @Override
    public void asyncAddEntry(byte[] bytes, AddEntryCallback addEntryCallback, Object ctx) {
        ledger.asyncAddEntry(bytes, addEntryCallback, ctx);
    }

    @Override
    public Position addEntry(byte[] bytes, int i, int i1) throws InterruptedException, ManagedLedgerException {
        return ledger.addEntry(bytes, i, i1);
    }

    @Override
    public Position addEntry(byte[] data, int numberOfMessages, int offset, int length) throws InterruptedException,
            ManagedLedgerException {
        return null;
    }

    @Override
    public void asyncAddEntry(byte[] bytes, int i, int i1, AddEntryCallback addEntryCallback, Object ctx) {
        ledger.asyncAddEntry(bytes, i, i1, addEntryCallback, ctx);
    }

    @Override
    public void asyncAddEntry(byte[] data, int numberOfMessages, int offset, int length, AddEntryCallback callback,
                              Object ctx) {
        ledger.asyncAddEntry(data, numberOfMessages, offset, length, callback, ctx);
    }

    @Override
    public void asyncAddEntry(ByteBuf byteBuf, AddEntryCallback addEntryCallback, Object ctx) {
        ledger.asyncAddEntry(byteBuf, addEntryCallback, ctx);
    }

    @Override
    public void asyncAddEntry(ByteBuf buffer, int numberOfMessages, AddEntryCallback callback, Object ctx) {
        ledger.asyncAddEntry(buffer, numberOfMessages, callback, ctx);
    }

    @Override
    public ManagedCursor openCursor(String s) throws InterruptedException, ManagedLedgerException {
        // TODO: return a TieredManagedCursor
        return null;
    }

    @Override
    public ManagedCursor openCursor(String s, InitialPosition initialPosition)
            throws InterruptedException, ManagedLedgerException {
        // TODO: return a TieredManagedCursor
        return null;
    }

    @Override
    public ManagedCursor openCursor(String s, InitialPosition initialPosition, Map<String, Long> map)
            throws InterruptedException, ManagedLedgerException {
        // TODO: return a TieredManagedCursor
        return null;
    }

    @Override
    public ManagedCursor newNonDurableCursor(Position position) throws ManagedLedgerException {
        // TODO: return a TieredManagedCursor
        return null;
    }

    @Override
    public ManagedCursor newNonDurableCursor(Position position, String s) throws ManagedLedgerException {
        // TODO: return a TieredManagedCursor
        return null;
    }

    @Override
    public ManagedCursor newNonDurableCursor(Position position, String s, InitialPosition initialPosition)
            throws ManagedLedgerException {
        // TODO: return a TieredManagedCursor
        return null;
    }

    @Override
    public void asyncDeleteCursor(String s, DeleteCursorCallback deleteCursorCallback, Object ctx) {
        ledger.asyncDeleteCursor(s, deleteCursorCallback, ctx);
    }

    @Override
    public void deleteCursor(String s) throws InterruptedException, ManagedLedgerException {
        ledger.deleteCursor(s);
    }

    @Override
    public void asyncOpenCursor(String s, OpenCursorCallback openCursorCallback, Object ctx) {
        // TODO: return a TieredManagedCursor
    }

    @Override
    public void asyncOpenCursor(
            String s, InitialPosition initialPosition, OpenCursorCallback openCursorCallback, Object ctx) {
        // TODO: return a TieredManagedCursor
    }

    @Override
    public void asyncOpenCursor(
            String s, InitialPosition initialPosition,
            Map<String, Long> map, OpenCursorCallback openCursorCallback, Object o) {
        // TODO: return a TieredManagedCursor
    }

    @Override
    public Iterable<ManagedCursor> getCursors() {
        return ledger.getCursors();
    }

    @Override
    public Iterable<ManagedCursor> getActiveCursors() {
        return ledger.getActiveCursors();
    }

    @Override
    public long getNumberOfEntries() {
        // TODO: implement the logic
        return 0;
    }

    @Override
    public long getNumberOfActiveEntries() {
        // TODO: implement the logic
        return 0;
    }

    @Override
    public long getTotalSize() {
        // TODO: implement the logic
        return 0;
    }

    @Override
    public long getEstimatedBacklogSize() {
        // TODO: implement the logic
        return 0;
    }

    @Override
    public long getOffloadedSize() {
        // TODO: implement the logic
        return 0;
    }

    @Override
    public void asyncTerminate(TerminateCallback terminateCallback, Object ctx) {
        ledger.asyncTerminate(terminateCallback, ctx);
    }

    @Override
    public Position terminate() throws InterruptedException, ManagedLedgerException {
        return ledger.terminate();
    }

    @Override
    public void close() throws InterruptedException, ManagedLedgerException {
        // TODO: implement the logic
        ledger.close();
    }

    @Override
    public void asyncClose(CloseCallback closeCallback, Object ctx) {
        // TODO: implement the logic
        ledger.asyncClose(closeCallback, ctx);
    }

    @Override
    public ManagedLedgerMXBean getStats() {
        return ledger.getStats();
    }

    @Override
    public void doCacheEviction(long maxTimestamp) {
        ledger.doCacheEviction(maxTimestamp);
    }

    @Override
    public void delete() throws InterruptedException, ManagedLedgerException {
        // TODO: implement the logic to delete data from tiered storage.
        ledger.delete();
    }

    @Override
    public void asyncDelete(DeleteLedgerCallback deleteLedgerCallback, Object ctx) {
        // TODO: implement the logic to delete data from tiered storage.
        ledger.asyncDelete(deleteLedgerCallback, ctx);
    }

    @Override
    public Position offloadPrefix(Position position) throws InterruptedException, ManagedLedgerException {
        throw new UnsupportedOperationException("Existing offload mechanism is disabled");
    }

    @Override
    public void asyncOffloadPrefix(Position position, OffloadCallback offloadCallback, Object o) {
        throw new UnsupportedOperationException("Existing offload mechanism is disabled");
    }

    @Override
    public ManagedCursor getSlowestConsumer() {
        return ledger.getSlowestConsumer();
    }

    @Override
    public boolean isTerminated() {
        return ledger.isTerminated();
    }

    @Override
    public ManagedLedgerConfig getConfig() {
        return originalConfig;
    }

    @Override
    public void setConfig(ManagedLedgerConfig managedLedgerConfig) {
        this.originalConfig = managedLedgerConfig;
        // TODO: disable offloader settings
        this.ledger.setConfig(managedLedgerConfig);
    }

    @Override
    public Position getLastConfirmedEntry() {
        return ledger.getLastConfirmedEntry();
    }

    @Override
    public String getState() {
        return ledger.getState();
    }

    @Override
    public void readyToCreateNewLedger() {
        ledger.readyToCreateNewLedger();
    }

    @Override
    public Map<String, String> getProperties() {
        return ledger.getProperties();
    }

    @Override
    public void setProperty(String key, String value) throws InterruptedException, ManagedLedgerException {
        ledger.setProperty(key, value);
    }

    @Override
    public void asyncSetProperty(String key, String value,
                                 UpdatePropertiesCallback updatePropertiesCallback, Object ctx) {
        ledger.asyncSetProperty(key, value, updatePropertiesCallback, ctx);
    }

    @Override
    public void deleteProperty(String key) throws InterruptedException, ManagedLedgerException {
        ledger.deleteProperty(key);
    }

    @Override
    public void asyncDeleteProperty(String key, UpdatePropertiesCallback updatePropertiesCallback, Object ctx) {
        ledger.asyncDeleteProperty(key, updatePropertiesCallback, ctx);
    }

    @Override
    public void setProperties(Map<String, String> properties) throws InterruptedException, ManagedLedgerException {
        ledger.setProperties(properties);
    }

    @Override
    public void asyncSetProperties(Map<String, String> properties,
                                   UpdatePropertiesCallback updatePropertiesCallback,
                                   Object ctx) {
        ledger.asyncSetProperties(properties, updatePropertiesCallback, ctx);
    }

    @Override
    public void trimConsumedLedgersInBackground(CompletableFuture<?> completableFuture) {
        // TODO: implement the trim logic
    }

    @Override
    public void rollCurrentLedgerIfFull() {
        ledger.rollCurrentLedgerIfFull();
    }

    @Override
    public CompletableFuture<Position> asyncFindPosition(Predicate<Entry> predicate) {
        return ledger.asyncFindPosition(predicate);
    }

    @Override
    public ManagedLedgerInterceptor getManagedLedgerInterceptor() {
        return ledger.getManagedLedgerInterceptor();
    }

    @Override
    public CompletableFuture<MLDataFormats.ManagedLedgerInfo.LedgerInfo> getLedgerInfo(long ledgerId) {
        return ledger.getLedgerInfo(ledgerId);
    }
}
