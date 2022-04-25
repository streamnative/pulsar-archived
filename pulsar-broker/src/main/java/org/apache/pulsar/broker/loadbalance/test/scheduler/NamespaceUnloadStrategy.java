package org.apache.pulsar.broker.loadbalance.test.scheduler;

import com.google.common.collect.Multimap;
import org.apache.pulsar.broker.loadbalance.test.BaseLoadManagerContext;

/**
 * The namespace unload strategy.
 * Used to determine the criteria for unloading bundles.
 *
 * Migrate from {@link org.apache.pulsar.broker.loadbalance.LoadSheddingStrategy}
 */
public interface NamespaceUnloadStrategy {

    Multimap<String, String> findBundlesForUnloading(BaseLoadManagerContext context);
}
