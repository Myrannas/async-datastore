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

import com.google.datastore.v1.Projection
import com.google.datastore.v1.PropertyReference
import com.google.protobuf.ByteString
import com.google.protobuf.Int32Value

/**
 * A query statement.

 * Retrieves one or more entities that satisfy a given criteria and order.
 */
class Query internal constructor() : Statement {

    private val query: com.google.datastore.v1.Query.Builder = com.google.datastore.v1.Query.newBuilder()
    private val filters: MutableList<Filter> = mutableListOf()

    /**
     * Specifies that only entity keys should be returned and not all
     * properties.

     * @return this query statement.
     */
    fun keysOnly(): Query {
        query.addProjection(
                Projection
                        .newBuilder()
                        .setProperty(PropertyReference.newBuilder().setName("__key__").build()))
        return this
    }

    /**
     * Only return a given set of properties, otherwise known as a Projection Query.

     * @param properties one or more property names to return.
     * *
     * @return this query statement.
     */
    fun properties(vararg properties: String) = properties(listOf(*properties))

    /**
     * Only return a given set of properties, otherwise known as a Projection Query.

     * @param properties one or more property names to return.
     * *
     * @return this query statement.
     */
    fun properties(properties: List<String>): Query {
        query.addAllProjection(properties
                .map { property ->
                    Projection.newBuilder()
                            .setProperty(com.google.datastore.v1.PropertyReference
                                    .newBuilder()
                                    .setName(property))
                            .build()
                })
        return this
    }

    /**
     * Query entities of a given kind.

     * @param kind the kind of entity to query.
     * *
     * @return this query statement.
     */
    fun kindOf(kind: String): Query {
        query.addKind(com.google.datastore.v1.KindExpression.newBuilder().setName(kind))
        return this
    }

    /**
     * Apply a given filter to the query.

     * @param filter the query filter to apply.
     * *
     * @return this query statement.
     */
    fun filterBy(filter: Filter): Query {
        filters.add(filter)
        return this
    }

    /**
     * Apply a given order to the query.

     * @param order the query order to apply.
     * *
     * @return this query statement.
     */
    fun orderBy(order: Order): Query {
        query.addOrder(order.pb)
        return this
    }

    /**
     * Apply a given group to the query.

     * @param group the query group to apply.
     * *
     * @return this query statement.
     */
    fun groupBy(group: Group): Query {
        query.addDistinctOn(group.pb)
        return this
    }

    /**
     * Tell Datastore to begin returning entities from a given cursor
     * position. This is used to page results; the last cursor position
     * is returned in `QueryResult`.

     * @param cursor the last query cursor position.
     * *
     * @return this query statement.
     */
    fun fromCursor(cursor: ByteString): Query {
        query.startCursor = cursor
        return this
    }

    /**
     * Limit the number of entities returned in this query. The last
     * cursor position will be returned in `QueryResult` if more
     * entities are required.

     * @param limit the maximum number of entities to return.
     * *
     * @return this query statement.
     */
    fun limit(limit: Int): Query {
        query.setLimit(Int32Value.newBuilder().setValue(limit))
        return this
    }

    internal fun getPb(namespace: String): com.google.datastore.v1.Query {
        if (filters.size == 1) {
            query.filter = filters[0].getPb(namespace)
        } else if (filters.size > 1) {
            query.setFilter(com.google.datastore.v1.Filter.newBuilder()
                    .setCompositeFilter(
                            com.google.datastore.v1.CompositeFilter.newBuilder()
                                    .addAllFilters(filters.map { it.getPb(namespace) })
                                    .setOp(com.google.datastore.v1.CompositeFilter.Operator.AND)))
        }
        return query.build()
    }
}
