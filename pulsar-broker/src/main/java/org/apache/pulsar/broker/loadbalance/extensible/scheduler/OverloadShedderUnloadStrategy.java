package org.apache.pulsar.broker.loadbalance.extensible.scheduler;

import com.google.common.collect.Multimap;
import org.apache.pulsar.broker.loadbalance.extensible.BaseLoadManagerContext;

public class OverloadShedderUnloadStrategy implements NamespaceUnloadStrategy {
    @Override
    public Multimap<String, String> findBundlesForUnloading(BaseLoadManagerContext context) {
        return null;
    }
}
