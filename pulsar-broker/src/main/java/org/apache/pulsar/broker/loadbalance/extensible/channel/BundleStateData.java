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
package org.apache.pulsar.broker.loadbalance.extensible.channel;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.Objects;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class BundleStateData {

    private String bundle;
    private BundleState state;
    private String broker;
    @JsonProperty("src_broker")
    private String sourceBroker;
    private long timestamp;

    public BundleStateData(String bundle, BundleState state, String broker) {
        this(bundle, state, broker, null);
    }

    public BundleStateData(String bundle, BundleState state, String broker, String srcBroker) {
        this.bundle = bundle;
        this.state = state;
        this.broker = broker;
        this.sourceBroker = srcBroker;
        this.timestamp = System.currentTimeMillis();
    }


    // test only
    public BundleStateData(BundleState state, String broker) {
        this.state = state;
        this.broker = broker;
        this.sourceBroker = null;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        BundleStateData that = (BundleStateData) o;
        return bundle.equals(that.bundle);
    }

    @Override
    public int hashCode() {
        return Objects.hash(bundle);
    }
}
