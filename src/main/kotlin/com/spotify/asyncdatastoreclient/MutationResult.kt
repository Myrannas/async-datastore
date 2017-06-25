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

import com.google.datastore.v1.MutationResult as PMutationResult

/**
 * A query result.

 * Returned from all mutation operations.
 */
class MutationResult(private val results: List<PMutationResult>, val indexUpdates: Int) : Result {

    /**
     * Return the first entity key that was inserted or null if empty.

     * This is a shortcut for `getInsertKeys().get(0)`

     * @return a key that describes the newly inserted entity.
     */
    val insertKey: Key?
        get() {
            if (results.isEmpty()) {
                return null
            }
            return Key.builder(results[0].key).build()
        }

    /**
     * Return all entity keys that were inserted for automatically generated
     * key ids.

     * @return a list of keys that describe the newly inserted entities.
     */
    val insertKeys: List<Key>
        get() = results.map { Key.builder(it.key).build() }

    companion object {

        internal fun build(response: com.google.datastore.v1.CommitResponse): MutationResult {
            return MutationResult(response.mutationResultsList, response.indexUpdates)
        }

        /**
         * Build an empty result.

         * @return a new empty mutation result.
         */
        fun build(): MutationResult {
            return MutationResult(listOf(PMutationResult.getDefaultInstance()), 0)
        }
    }
}
