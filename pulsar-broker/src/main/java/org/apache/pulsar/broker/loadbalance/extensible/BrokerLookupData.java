package org.apache.pulsar.broker.loadbalance.extensible;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
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
}
