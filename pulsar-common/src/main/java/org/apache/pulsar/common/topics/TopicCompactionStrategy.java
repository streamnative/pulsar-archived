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
package org.apache.pulsar.common.topics;

import org.apache.pulsar.client.api.Schema;

public interface TopicCompactionStrategy<T> {

    /**
     * Returns the schema object for this strategy.
     * @return
     */
    Schema<T> getSchema();
    /**
     * Tests if the current message is valid compared to the previous message for the same key.
     *
     * @param prev previous message
     * @param cur current message
     * @return True if the prev to the cur message transition is valid. Otherwise, false.
     */
    boolean isValid(T prev, T cur);

    /**
     * Returns true if this strategy supports merge.
     * Warning: `merge` will make the topic compaction more complex(requiring more cpu and memory load),
     * as the compaction process needs to cache the previous messages to merge.
     */
    boolean isMergeEnabled();

    /**
     * Merges the prev and cur messages for the same key, if isValid() and isMergeEnabled() return true.
     *
     * @param prev previous message
     * @param cur current message
     * @return merged message
     */
    T merge(T prev, T cur);


    static TopicCompactionStrategy load(String topicCompactionStrategy) {
        if (topicCompactionStrategy == null) {
            return null;
        }
        try {
            //
            Class<?> clazz = Class.forName(topicCompactionStrategy);
            Object loadSheddingInstance = clazz.getDeclaredConstructor().newInstance();
            return (TopicCompactionStrategy) loadSheddingInstance;
        } catch (Exception e) {
            throw new IllegalArgumentException("Error when loading topic compaction strategy.", e);
        }
    }
}
