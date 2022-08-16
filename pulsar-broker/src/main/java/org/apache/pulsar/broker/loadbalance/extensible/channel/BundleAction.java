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

/**
 * The bundle actions.
 */
public enum BundleAction {

    /**
     * Own the bundle ownership. The owner broker is selected by the local load manager.
     */
    Own,

    /**
     * Transfer the bundle ownership to the destination broker.
     *  The source broker internally disables the bundle ownership.
     *  The destination broker owns the bundle.
     */
    Transfer,

    /**
     * Return deferred client connections with the destination broker URL.
     * Close the connections if already being served
     */
    Return,

    /**
     * Split the target(parent) bundle into child bundles.
     */
    Split,

    /**
     * Create the child bundle entries in the channel, initially assigned to the local broker.
     */
    Create,

    /**
     * Discard the bundle entry in the channel(tombstone operation).
     */
    Discard,

    /**
     * Unload the bundle ownership from the owner broker.
     *   Disable the bundle ownership
     *   Close client connections under the bundle
     *   Run the Discard action
     */
    Unload
}
