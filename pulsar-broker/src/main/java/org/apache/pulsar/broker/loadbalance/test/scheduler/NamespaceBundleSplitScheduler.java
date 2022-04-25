package org.apache.pulsar.broker.loadbalance.test.scheduler;

import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.loadbalance.impl.LoadManagerShared;
import org.apache.pulsar.broker.loadbalance.test.BaseLoadManagerContext;
import org.apache.pulsar.common.naming.NamespaceBundleFactory;
import org.apache.pulsar.common.naming.NamespaceName;
import java.util.Set;

/**
 * Split bundle scheduler.
 */
@Slf4j
public class NamespaceBundleSplitScheduler implements LoadManagerScheduler {

    private final PulsarService pulsar;

    private final BaseLoadManagerContext context;

    private final ServiceConfiguration conf;

    private final NamespaceBundleSplitStrategy bundleSplitStrategy;


    public NamespaceBundleSplitScheduler(PulsarService pulsar,
                                         BaseLoadManagerContext context) {
        this.pulsar = pulsar;
        this.context = context;
        this.conf = context.brokerConfiguration();
        this.bundleSplitStrategy = null;
    }


    @Override
    public void execute() {
        if (!this.isLoadBalancerAutoBundleSplitEnabled() || !this.isLeader()) {
            return;
        }
        // TODO: Do we need to check the available broker's size equals the load data size?
        if (context.brokerRegistry().getAvailableBrokers().size() != context.brokerLoadDataStore().size()) {
            log.info("The load data synchronization is in progress, skip the bundle split.");
            return;
        }
        final boolean unloadSplitBundles =
                pulsar.getConfiguration().isLoadBalancerAutoUnloadSplitBundlesEnabled();
        synchronized (bundleSplitStrategy) {
            final Set<String> bundlesToBeSplit =
                    bundleSplitStrategy.findBundlesToSplit(context, pulsar.getNamespaceService());
            NamespaceBundleFactory namespaceBundleFactory =
                    pulsar.getNamespaceService().getNamespaceBundleFactory();
            for (String bundleName : bundlesToBeSplit) {
                try {
                    final String namespaceName = LoadManagerShared.getNamespaceNameFromBundleName(bundleName);
                    final String bundleRange = LoadManagerShared.getBundleRangeFromBundleName(bundleName);
                    if (!namespaceBundleFactory
                            .canSplitBundle(namespaceBundleFactory.getBundle(namespaceName, bundleRange))) {
                        continue;
                    }

                    // Make sure the same bundle is not selected again.
                    context.bundleLoadDataStore().remove(bundleName);
                    // Clear namespace bundle-cache
                    namespaceBundleFactory
                            .invalidateBundleCache(NamespaceName.get(namespaceName));

                    log.info("Load-manager splitting bundle {} and unloading {}", bundleName, unloadSplitBundles);
                    // TODO: Should we check if the namespace bundle is already split?
                    pulsar.getAdminClient().namespaces().splitNamespaceBundle(namespaceName, bundleRange,
                            unloadSplitBundles, null);

                    log.info("Successfully split namespace bundle {}", bundleName);
                } catch (Exception e) {
                    log.error("Failed to split namespace bundle {}", bundleName, e);
                }
            }
        }
    }

    private boolean isLoadBalancerAutoBundleSplitEnabled() {
        return conf.isLoadBalancerAutoBundleSplitEnabled();
    }

    private boolean isLeader() {
        return pulsar.getLeaderElectionService() != null && pulsar.getLeaderElectionService().isLeader();
    }
}
