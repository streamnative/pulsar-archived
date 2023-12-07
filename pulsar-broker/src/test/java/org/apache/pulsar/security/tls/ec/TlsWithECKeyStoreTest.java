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
package org.apache.pulsar.security.tls.ec;


import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import lombok.Cleanup;
import lombok.SneakyThrows;
import org.apache.pulsar.broker.auth.MockedPulsarServiceBaseTest;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.impl.auth.AuthenticationKeyStoreTls;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;


@Test
public class TlsWithECKeyStoreTest extends MockedPulsarServiceBaseTest {
    @Override
    protected void doInitConf() throws Exception {
        super.doInitConf();
        conf.setTlsEnabled(true);
        conf.setBrokerServicePort(Optional.empty());
        conf.setWebServicePort(Optional.empty());
        conf.setTlsEnabledWithKeyStore(true);
        conf.setTlsKeyStore(TLS_EC_KS_SERVER_STORE);
        conf.setTlsKeyStorePassword(TLS_EC_KS_SERVER_PASS);
        conf.setTlsTrustStore(TLS_EC_KS_TRUSTED_STORE);
        conf.setTlsTrustStorePassword(TLS_EC_KS_TRUSTED_STORE_PASS);
        conf.setTlsRequireTrustedClientCertOnConnect(true);
        conf.setBrokerClientTlsEnabled(true);
        conf.setBrokerClientTlsEnabledWithKeyStore(true);
        conf.setBrokerClientTlsTrustStore(TLS_EC_KS_TRUSTED_STORE);
        conf.setBrokerClientTlsTrustStorePassword(TLS_EC_KS_TRUSTED_STORE_PASS);
        conf.setBrokerClientAuthenticationPlugin(AuthenticationKeyStoreTls.class.getName());
        final Map<String, String> brokerClientAuthParams = new HashMap<>();
        brokerClientAuthParams.put("keyStorePath", TLS_EC_KS_BROKER_CLIENT_STORE);
        brokerClientAuthParams.put("keyStorePassword", TLS_EC_KS_BROKER_CLIENT_PASS);
        conf.setBrokerClientAuthenticationParameters(mapper.writeValueAsString(brokerClientAuthParams));
    }

    @BeforeClass(alwaysRun = true)
    @Override
    protected void setup() throws Exception {
        init();
        setupDefaultTenantAndNamespace();
    }

    @AfterClass(alwaysRun = true)
    @Override
    protected void cleanup() throws Exception {
        cleanup();
    }

    @Test(expectedExceptions = PulsarClientException.class)
    @SneakyThrows
    public void testConnectionFailWithoutCertificate() {
        @Cleanup final PulsarClient client = PulsarClient.builder()
                .serviceUrl(pulsar.getBrokerServiceUrlTls())
                .build();
        @Cleanup final Producer<byte[]> producer = client.newProducer()
                .topic("should_be_failed")
                .create();
    }


    @Test
    @SneakyThrows
    public void testConnectionSuccessWithCertificate() {
        final String topicName = "persistent://public/default/" + UUID.randomUUID();
        final int testMsgNum = 10;
        final Map<String, String> clientAuthParams = new HashMap<>();
        clientAuthParams.put("keyStorePath", TLS_EC_KS_CLIENT_STORE);
        clientAuthParams.put("keyStorePassword", TLS_EC_KS_CLIENT_PASS);
        @Cleanup final PulsarAdmin admin = PulsarAdmin.builder()
                .useKeyStoreTls(true)
                .tlsTrustStorePath(TLS_EC_KS_TRUSTED_STORE)
                .tlsTrustStorePassword(TLS_EC_KS_TRUSTED_STORE_PASS)
                .authentication(AuthenticationKeyStoreTls.class.getName(), mapper.writeValueAsString(clientAuthParams))
                .serviceHttpUrl(pulsar.getWebServiceAddressTls())
                .build();
        admin.topics().createNonPartitionedTopic(topicName);
        admin.topics().createSubscription(topicName, "sub-1", MessageId.earliest);
        @Cleanup final PulsarClient client = PulsarClient.builder()
                .serviceUrl(pulsar.getBrokerServiceUrlTls())
                .useKeyStoreTls(true)
                .tlsTrustStorePath(TLS_EC_KS_TRUSTED_STORE)
                .tlsTrustStorePassword(TLS_EC_KS_TRUSTED_STORE_PASS)
                .authentication(AuthenticationKeyStoreTls.class.getName(), mapper.writeValueAsString(clientAuthParams))
                .build();
        @Cleanup final Producer<byte[]> producer = client.newProducer()
                .topic(topicName)
                .create();
        @Cleanup final Consumer<byte[]> consumer = client.newConsumer()
                .topic(topicName)
                .subscriptionName("sub-1")
                .consumerName("cons-1")
                .subscribe();
        for (int i = 0; i < testMsgNum; i++) {
            producer.send((i + "").getBytes(StandardCharsets.UTF_8));
        }

        for (int i = 0; i < testMsgNum; i++) {
            final Message<byte[]> message = consumer.receive();
            assertNotNull(message);
            final byte[] b = message.getValue();
            final String s = new String(b, StandardCharsets.UTF_8);
            assertEquals(s, i + "");
        }
    }

}
