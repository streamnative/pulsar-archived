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
package org.apache.pulsar.broker.loadbalance.extensible.filter;

import com.github.zafarkhaja.semver.Version;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.mutable.MutableObject;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.loadbalance.BrokerFilterBadVersionException;
import org.apache.pulsar.broker.loadbalance.BrokerFilterException;
import org.apache.pulsar.broker.loadbalance.extensible.BaseLoadManagerContext;
import org.apache.pulsar.broker.loadbalance.extensible.BrokerLookupData;
import org.apache.pulsar.broker.loadbalance.extensible.BrokerRegistry;

/**
 * Filter by broker version.
 */
@Slf4j
public class BrokerVersionFilter extends BaseBrokerFilter {

    public static final String FILTER_NAME = "broker_version_filter";

    @Override
    void doFilter(List<String> brokers, BaseLoadManagerContext context) throws BrokerFilterException {
        ServiceConfiguration conf = context.brokerConfiguration();
        if (!conf.isPreferLaterVersions()) {
            return;
        }

        BrokerRegistry brokerRegistry = context.brokerRegistry();
        Version latestVersion;
        try {
            latestVersion = getLatestVersionNumber(brokers, brokerRegistry);
            log.info("Latest broker version found was [{}]", latestVersion);
        } catch (Exception ex) {
            log.warn("Disabling PreferLaterVersions feature; reason: " + ex.getMessage());
            throw new BrokerFilterBadVersionException("Cannot determine newest broker version: " + ex.getMessage());
        }

        int numBrokersLatestVersion = 0;
        int numBrokersOlderVersion = 0;
        Iterator<String> brokerIterator = brokers.iterator();
        while (brokerIterator.hasNext()) {
            String broker = brokerIterator.next();
            Optional<BrokerLookupData> lookupDataOpt = brokerRegistry.lookup(broker);
            if (lookupDataOpt.isPresent()) {
                String brokerVersion = lookupDataOpt.get().getBrokerVersion();
                Version brokerVersionVersion = Version.valueOf(brokerVersion);

                if (brokerVersionVersion.equals(latestVersion)) {
                    log.debug("Broker [{}] is running the latest version ([{}])", broker, brokerVersion);
                    ++numBrokersLatestVersion;
                } else {
                    log.info("Broker [{}] is running an older version ([{}]); latest version is [{}]",
                            broker, brokerVersion, latestVersion);
                    ++numBrokersOlderVersion;
                    brokerIterator.remove();
                }
            }

        }
        if (numBrokersOlderVersion == 0) {
            log.info("All {} brokers are running the latest version [{}]", numBrokersLatestVersion, latestVersion);
        }
    }

    /**
     * Get the most recent broker version number from the broker lookup data of all the running brokers.
     * The version number is from the build artifact in the pom and got added to the package when it was built by Maven
     *
     * @param brokers
     *            The brokers to choose the latest version string from.
     * @param registry
     *            The broker registry from the leader broker (Contains the broker lookupData)
     * @return The most recent broker version
     * @throws BrokerFilterBadVersionException
     *            If the most recent version is undefined (e.g., a bad broker version was encountered or a broker
     *            does not have a version string in its load report.
     */
    public Version getLatestVersionNumber(List<String> brokers, BrokerRegistry registry)
            throws BrokerFilterBadVersionException {

        if (null == brokers) {
            throw new BrokerFilterBadVersionException("Unable to determine latest version since broker set was null");
        }
        if (brokers.size() == 0) {
            throw new BrokerFilterBadVersionException("Unable to determine latest version since broker set was empty");
        }
        if (null == registry) {
            throw new BrokerFilterBadVersionException("Unable to determine latest version since registry was null");
        }

        final MutableObject<Version> latestVersion = new MutableObject<>(null);
        try {
            registry.forEach((broker, lookupData) -> {
                String brokerVersion = lookupData.getBrokerVersion();
                if (null == brokerVersion || brokerVersion.length() == 0) {
                    log.warn("No version string in load report for broker [{}]; disabling PreferLaterVersions feature",
                            broker);
                    // trigger the ModularLoadManager to reset all the brokers to the original set
                    throw new RuntimeException("No version string in load report for broker \""
                            + broker + "\"");
                }
                Version brokerVersionVersion;
                try {
                    brokerVersionVersion = Version.valueOf(brokerVersion);
                } catch (Exception x) {
                    log.warn("Invalid version string in load report for broker [{}]: [{}];"
                                    + " disabling PreferLaterVersions feature",
                            broker, brokerVersion);
                    // trigger the ModularLoadManager to reset all the brokers to the original set
                    throw new RuntimeException("Invalid version string in load report for broker \""
                            + broker + "\": \"" + brokerVersion + "\")");
                }

                if (latestVersion.getValue() == null) {
                    latestVersion.setValue(brokerVersionVersion);
                } else if (Version.BUILD_AWARE_ORDER.compare(latestVersion.getValue(), brokerVersionVersion) < 0) {
                    latestVersion.setValue(brokerVersionVersion);
                }
            });
        } catch (RuntimeException ex) {
            // Throw runtime exception to jump out the forEach loop.
            throw new BrokerFilterBadVersionException(ex.getMessage());
        }

        return latestVersion.getValue();
    }


    @Override
    public String name() {
        return FILTER_NAME;
    }
}
