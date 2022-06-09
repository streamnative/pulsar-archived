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
package org.apache.pulsar.broker.loadbalance.extensible.data;

import org.apache.pulsar.broker.PulsarServerException;
import org.apache.pulsar.broker.PulsarService;

/**
 * The load data store factory, use to create the load data store.
 */
public class LoadDataStoreFactory {

    public static final String TABLEVIEW_STORE = "TableViewStore";

    public static final String METADATA_STORE = "MetaDataStore";

    public static <T> LoadDataStore<T> create(PulsarService pulsar, String name, Class<T> clazz)
        throws LoadDataStoreException {
        return newInstance(pulsar, pulsar.getConfiguration().getLoadDataStoreName(), name, clazz);
    }

    protected static <T> LoadDataStore<T> newInstance(PulsarService pulsar,
                                                    String loadDataStoreName,
                                                    String name,
                                                    Class<T> clazz)
        throws LoadDataStoreException {
        try {
            if (TABLEVIEW_STORE.equals(loadDataStoreName)) {
                return new TableViewLoadDataStoreImpl<>(pulsar.getClient(), name, clazz);
            } else if (METADATA_STORE.equals(loadDataStoreName)) {
                return new MsLoadDataStoreImpl<>(pulsar.getLocalMetadataStore(), name, clazz);
            } else {
                return new TableViewLoadDataStoreImpl<>(pulsar.getClient(), name, clazz);
            }
        } catch (PulsarServerException ex) {
            throw new LoadDataStoreException(ex);
        }

    }
}
