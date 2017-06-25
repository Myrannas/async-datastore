/*
 * Copyright (c) 2011-2015 Spotify AB
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package com.spotify.asyncdatastoreclient

/**
 * An allocate ids statement.

 * A allocate ids operation sent to Datastore.
 */
class AllocateIds(val keys: MutableList<Key> = mutableListOf()) : Statement {

    /**
     * Adds a partial key for allocation.

     * @param kind the partial key kind.
     * *
     * @return this allocate ids statement.
     */
    fun add(kind: String): AllocateIds {
        keys.add(Key.builder(kind).build())
        return this
    }

    /**
     * Adds a partial key for allocation.

     * @param key the partial key.
     * *
     * @return this allocate ids statement.
     */
    fun add(key: Key): AllocateIds {
        keys.add(key)
        return this
    }

    internal fun getPb(namespace: String) = keys.map { Key.builder(it).build().getPb(namespace) }

}
