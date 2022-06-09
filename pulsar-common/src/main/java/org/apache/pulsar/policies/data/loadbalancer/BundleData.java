/*
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
package org.apache.pulsar.policies.data.loadbalancer;

/**
 * Data class comprising the short term and long term historical data for this bundle.
 */
public class BundleData {

    // The number of effective samples to keep for observing long term data.
    public static final int NUM_LONG_SAMPLES = 1000;

    // The number of effective samples to keep for observing short term data.
    public static final int NUM_SHORT_SAMPLES = 10;

    // The default bundle stats which are used to initialize historic data.
    // This data is overridden after the bundle receives its first sample.
    private static final NamespaceBundleStats DEFAULT_NAMESPACE_BUNDLE_STATS =
            NamespaceBundleStats.newDefaultNamespaceBundleStats();

    // Short term data for this bundle. The time frame of this data is
    // determined by the number of short term samples
    // and the bundle update period.
    private TimeAverageMessageData shortTermData;

    // Long term data for this bundle. The time frame of this data is determined
    // by the number of long term samples
    // and the bundle update period.
    private TimeAverageMessageData longTermData;

    // number of topics present under this bundle
    private int topics;

    public static BundleData newDefaultBundleData() {
        return new BundleData(NUM_SHORT_SAMPLES, NUM_LONG_SAMPLES, DEFAULT_NAMESPACE_BUNDLE_STATS);
    }

    // For JSON only.
    public BundleData() {
    }

    /**
     * Initialize the bundle data.
     *
     * @param numShortSamples
     *            Number of short term samples to use.
     * @param numLongSamples
     *            Number of long term samples to use.
     */
    public BundleData(final int numShortSamples, final int numLongSamples) {
        shortTermData = new TimeAverageMessageData(numShortSamples);
        longTermData = new TimeAverageMessageData(numLongSamples);
    }

    /**
     * Initialize this bundle data and have its histories default to the given stats before the first sample is
     * received.
     *
     * @param numShortSamples
     *            Number of short term samples to use.
     * @param numLongSamples
     *            Number of long term samples to use.
     * @param defaultStats
     *            The stats to default to before the first sample is received.
     */
    public BundleData(final int numShortSamples, final int numLongSamples, final NamespaceBundleStats defaultStats) {
        shortTermData = new TimeAverageMessageData(numShortSamples, defaultStats);
        longTermData = new TimeAverageMessageData(numLongSamples, defaultStats);
    }

    /**
     * Update the historical data for this bundle.
     *
     * @param newSample
     *            The bundle stats to update this data with.
     */
    public void update(final NamespaceBundleStats newSample) {
        shortTermData.update(newSample);
        longTermData.update(newSample);
        this.topics = (int) newSample.topics;
    }

    public TimeAverageMessageData getShortTermData() {
        return shortTermData;
    }

    public void setShortTermData(TimeAverageMessageData shortTermData) {
        this.shortTermData = shortTermData;
    }

    public TimeAverageMessageData getLongTermData() {
        return longTermData;
    }

    public void setLongTermData(TimeAverageMessageData longTermData) {
        this.longTermData = longTermData;
    }

    public int getTopics() {
        return topics;
    }

    public void setTopics(int topics) {
        this.topics = topics;
    }
}
