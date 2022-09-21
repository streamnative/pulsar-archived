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

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public enum BundleState {

    Assigned,

    Assigning,

    Closed,

    Splitting,

    Unloading;
    private static Map<BundleState, Set<BundleState>> validTransitions = new HashMap<>() {{
        put(null, new HashSet<>() {{
            add(Assigned); // from split
            add(Assigning); // from assignment
            //add(null); // from recovery
        }});
        put(Assigned, new HashSet<>() {{
            add(Assigning); // from transfer
            add(Unloading); // from unload
            add(Splitting); // from split
            add(null); // from recovery
        }});
        put(Assigning, new HashSet<>() {{
            add(Assigned); // from assignment
            add(Closed); // from transfer
            add(null); // from recovery

        }});

        put(Closed, new HashSet<>() {{
            add(Assigned); // from transfer
            add(null); // from recovery
        }});

        put(Splitting, new HashSet<>() {{
            add(null); // from split, from recovery
        }});

        put(Unloading, new HashSet<>() {{
            add(null); // from unload, from recovery
        }});

    }};

    public static boolean isValidTransition(BundleState from, BundleState to) {
        Set<BundleState> transitions = validTransitions.get(from);
        return transitions.contains(to);
    }

    public static Set<BundleState> inFlightStates = new HashSet<>(){{
        add(Assigning);
        add(Splitting);
        add(Unloading);
    }};
}
