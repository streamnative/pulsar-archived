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

import static org.apache.pulsar.broker.loadbalance.extensible.channel.BundleState.Assigned;
import static org.apache.pulsar.broker.loadbalance.extensible.channel.BundleState.Owned;
import org.apache.commons.lang.StringUtils;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.impl.schema.JSONSchema;
import org.apache.pulsar.common.topics.TopicCompactionStrategy;

public class BundleStateCompactionStrategy implements TopicCompactionStrategy<BundleStateData> {

    private final Schema<BundleStateData> schema;

    private boolean checkBrokers = true;
    public BundleStateCompactionStrategy(){
        schema = JSONSchema.of(BundleStateData.class);
    }

    @Override
    public Schema<BundleStateData> getSchema() {
        return schema;
    }

    public void checkBrokers(boolean check) {
        this.checkBrokers = check;
    }

    @Override
    public boolean isValid(BundleStateData from, BundleStateData to) {
        BundleState prevState = from == null ? null : from.getState();
        BundleState state = to == null ? null : to.getState();
        if (!BundleState.isValidTransition(prevState, state)) {
            return false;
        }

        if (checkBrokers) {
            if (prevState == Owned && state == Assigned) { // transfer source => dst check
                return StringUtils.equals(from.getBroker(), to.getSourceBroker());
            }

            if (prevState == Assigned && state == Owned) {
                return StringUtils.equals(from.getBroker(), to.getBroker())
                        && StringUtils.equals(from.getSourceBroker(), to.getSourceBroker());
            }
        }

        return true;
    }

    @Override
    public boolean isMergeEnabled() {
        return false;
    }

    @Override
    public BundleStateData merge(BundleStateData prev, BundleStateData cur) {
        throw new UnsupportedOperationException("merge is not supported.");
    }
}
