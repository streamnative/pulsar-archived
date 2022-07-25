package org.apache.pulsar.broker.loadbalance.extensible.data;

import org.apache.pulsar.policies.data.loadbalancer.NamespaceBundleStats;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.HashMap;
import java.util.Map;

import static org.testng.Assert.*;

public class TopBundlesLoadDataTest {

    @Test
    public void testTopBundlesLoadData() {
        Map<String, NamespaceBundleStats> bundleStats = new HashMap<>();
        NamespaceBundleStats stats1 = new NamespaceBundleStats();
        stats1.msgRateIn = 1000000;
        bundleStats.put("bundle-1", stats1);

        NamespaceBundleStats stats2 = new NamespaceBundleStats();
        stats2.msgRateIn = 10000;
        bundleStats.put("bundle-2", stats2);

        NamespaceBundleStats stats3 = new NamespaceBundleStats();
        stats3.msgRateIn = 1000;
        bundleStats.put("bundle-3", stats3);

        NamespaceBundleStats stats4 = new NamespaceBundleStats();
        stats4.msgRateIn = 10;
        bundleStats.put("bundle-4", stats4);

        TopBundlesLoadData topBundlesLoadData = TopBundlesLoadData.of(bundleStats, 3);
        Map.Entry<String, NamespaceBundleStats> top0 = topBundlesLoadData.getTopBundlesLoadData().get(0);
        Map.Entry<String, NamespaceBundleStats> top1 = topBundlesLoadData.getTopBundlesLoadData().get(1);
        Map.Entry<String, NamespaceBundleStats> top2 = topBundlesLoadData.getTopBundlesLoadData().get(2);

        assertEquals(top0.getKey(), "bundle-1");
        assertEquals(top1.getKey(), "bundle-2");
        assertEquals(top2.getKey(), "bundle-3");
    }
}