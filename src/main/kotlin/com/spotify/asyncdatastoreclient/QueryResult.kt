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

import com.google.common.collect.Iterables
import com.google.datastore.v1.LookupResponse
import com.google.datastore.v1.RunQueryResponse
import com.google.protobuf.ByteString

/**
 * A query result.

 * Returned from query operations.
 */
class QueryResult(
        /**
         * Return all entities returned from the query.

         * @return a list of entities returned from the query.
         */
        val all: List<Entity> = listOf(),

        /**
         * The last cursor position after returning all entities for this batch.

         * This is useful for paging; if you supply a `limit()`, then the
         * cursor can be used when retrieving the next batch: `QueryfromCursor()`.

         * @return the last cursor position.
         */
        val cursor: ByteString? = null) : Result, Iterable<Entity> {

    /**
     * Return the first entity returned from the query or null if not found.

     * This is a shortcut for `getAll().get(0)`

     * @return an entity returned from the Datastore.
     */
    val entity: Entity?
        get() = Iterables.getFirst(all, null)

    /**
     * An iterator for all entities returned from the query.

     * @return an entity iterator.
     */
    override fun iterator() = all.iterator()

    companion object {

        internal fun build(response: LookupResponse) =
                QueryResult(response.foundList.map {
                    Entity.builder(it.entity).build()
                })

        internal fun build(response: RunQueryResponse) = with(response.batch) {
            QueryResult(entityResultsList.map {
                Entity.builder(it.entity).build()
            }, endCursor)
        }

        /**
         * Build an empty result.

         * @return a new empty query result.
         */
        fun build() = QueryResult(listOf())
    }
}
