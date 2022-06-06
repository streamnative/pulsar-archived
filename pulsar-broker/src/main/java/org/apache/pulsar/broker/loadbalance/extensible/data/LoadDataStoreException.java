package org.apache.pulsar.broker.loadbalance.extensible.data;

import org.apache.pulsar.metadata.api.MetadataStoreException;

import java.io.IOException;

public class LoadDataStoreException extends IOException {

    public LoadDataStoreException(Throwable t) {
        super(t);
    }

    public LoadDataStoreException(String msg) {
        super(msg);
    }

    public LoadDataStoreException(String msg, Throwable t) {
        super(msg, t);
    }

    public static class InvalidPathException extends LoadDataStoreException {
        public InvalidPathException(String path) {
            super("Path(" + path + ") is invalid");
        }
    }
}
