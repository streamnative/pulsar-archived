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
package org.apache.pulsar.broker.loadbalance.extensible;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.pulsar.broker.lookup.LookupResult;
import org.apache.pulsar.policies.data.loadbalancer.AdvertisedListener;
import org.apache.pulsar.policies.data.loadbalancer.ServiceLookupData;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class BrokerLookupData implements ServiceLookupData {

    // URLs to satisfy contract of ServiceLookupData (used by NamespaceService).
    private String webServiceUrl;
    private String webServiceUrlTls;
    private String pulsarServiceUrl;
    private String pulsarServiceUrlTls;

    // the external protocol data advertised by protocol handlers.
    private Map<String, String> protocols = new HashMap<>();
    private Map<String, AdvertisedListener> advertisedListeners;

    private boolean persistentTopicsEnabled = true;
    private boolean nonPersistentTopicsEnabled = true;

    private String brokerVersion;

    public BrokerLookupData(String webServiceUrl, String webServiceUrlTls, String pulsarServiceUrl,
                            String pulsarServiceUrlTls, Map<String, AdvertisedListener>  advertisedListeners) {
        this.webServiceUrl = webServiceUrl;
        this.webServiceUrlTls = webServiceUrlTls;
        this.pulsarServiceUrl = pulsarServiceUrl;
        this.pulsarServiceUrlTls = pulsarServiceUrlTls;
        this.advertisedListeners = Map.copyOf(advertisedListeners);
    }

    @Override
    public String getWebServiceUrl() {
        return webServiceUrl;
    }

    @Override
    public String getWebServiceUrlTls() {
        return webServiceUrlTls;
    }

    @Override
    public String getPulsarServiceUrl() {
        return pulsarServiceUrl;
    }

    @Override
    public String getPulsarServiceUrlTls() {
        return pulsarServiceUrlTls;
    }

    @Override
    public Map<String, String> getProtocols() {
        return protocols;
    }

    @Override
    public Optional<String> getProtocol(String protocol) {
        return Optional.ofNullable(protocols.get(protocol));
    }

    public Map<String, AdvertisedListener> getAdvertisedListeners() {
        return advertisedListeners;
    }

    public void setAdvertisedListeners(Map<String, AdvertisedListener> advertisedListeners) {
        this.advertisedListeners = advertisedListeners;
    }

    public LookupResult toLookupResult() {
        return new LookupResult(pulsarServiceUrl, pulsarServiceUrlTls, webServiceUrl, webServiceUrlTls,
                LookupResult.Type.BrokerUrl, false);
    }
}
