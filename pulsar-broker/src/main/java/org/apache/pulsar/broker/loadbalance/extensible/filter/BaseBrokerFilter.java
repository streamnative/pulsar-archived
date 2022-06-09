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

import java.util.List;
import org.apache.pulsar.broker.loadbalance.BrokerFilterException;
import org.apache.pulsar.broker.loadbalance.extensible.BaseLoadManagerContext;
import org.apache.pulsar.broker.loadbalance.extensible.LoadManagerContext;

/**
 * The base broker filter only use BaseContext.
 */
public abstract class BaseBrokerFilter implements BrokerFilter {

    abstract void doFilter(List<String> brokers, BaseLoadManagerContext context) throws BrokerFilterException;

    @Override
    public void filter(List<String> brokers, LoadManagerContext context) throws BrokerFilterException {
        if (context instanceof BaseLoadManagerContext) {
            this.doFilter(brokers, (BaseLoadManagerContext) context);
        } else {
            throw new BrokerFilterException("The context must be BaseContext.");
        }
    }
}
