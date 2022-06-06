package org.apache.pulsar.broker.loadbalance.extensible.data;

import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.client.api.Schema;

/**
 * The load data store factory, use to create the load data store.
 */
public class LoadDataStoreFactory {

    private static final String TABLEVIEW_STORE = "TableViewStore";

    private static final String METADATA_STORE = "MetaDataStore";

    public static <T> LoadDataStore<T> create(PulsarService pulsar, String path, Class<T> clazz)
        throws LoadDataStoreException {
        return newInstance(pulsar, path, clazz);
    }

    private static <T> LoadDataStore<T> newInstance(PulsarService pulsar, String path, Class<T> clazz)
        throws LoadDataStoreException {
        String loadDataStoreName = pulsar.getConfiguration().getLoadDataStoreName();
        if (TABLEVIEW_STORE.equals(loadDataStoreName)) {
            return new TableViewLoadDataStoreImpl<>(path, pulsar, Schema.AVRO(clazz));
        } else if (METADATA_STORE.equals(loadDataStoreName)) {
            return new MsLoadDataStoreImpl<>(path);
        } else {
            return new TableViewLoadDataStoreImpl<>(path, pulsar, Schema.AVRO(clazz));
        }
    }
}
