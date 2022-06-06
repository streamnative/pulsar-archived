package org.apache.pulsar.broker.loadbalance.extensible.data;

import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.client.api.Schema;

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
        if (TABLEVIEW_STORE.equals(loadDataStoreName)) {
            return new TableViewLoadDataStoreImpl<>(name, pulsar, Schema.AVRO(clazz));
        } else if (METADATA_STORE.equals(loadDataStoreName)) {
            return new MsLoadDataStoreImpl<>(name);
        } else {
            return new TableViewLoadDataStoreImpl<>(name, pulsar, Schema.AVRO(clazz));
        }
    }
}
