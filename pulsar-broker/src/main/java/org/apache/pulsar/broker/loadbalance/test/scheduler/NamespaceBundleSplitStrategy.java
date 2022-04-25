package org.apache.pulsar.broker.loadbalance.test.scheduler;

import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.loadbalance.test.BaseLoadManagerContext;
import org.apache.pulsar.broker.namespace.NamespaceService;

import java.util.Set;

/**
 * Determines which bundles should be split based on various thresholds.
 *
 * Migrate from {@link org.apache.pulsar.broker.loadbalance.BundleSplitStrategy}
 */
public interface NamespaceBundleSplitStrategy {

    /**
     * Determines which bundles, if any, should be split.
     *
     * @param context
     *            Load data to base decisions on (does not have benefit of preallocated data since this may not be the
     *            leader broker).
     * @param namespaceService
     *            Namespace service to use.
     * @return A set of the bundles that should be split.
     */
    Set<String> findBundlesToSplit(BaseLoadManagerContext context, NamespaceService namespaceService);
}
