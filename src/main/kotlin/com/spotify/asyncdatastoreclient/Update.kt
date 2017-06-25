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

import com.google.datastore.v1.Mutation

/**
 * An update statement.

 * Update a single entity based on a given `Key` and properties.
 */
class Update : KeyedStatement, MutationStatement {

    private var upsert: Boolean = false

    protected var entity: Entity.Builder

    internal constructor(key: Key) : super(key) {
        this.entity = Entity.builder(key)
    }

    internal constructor(entity: Entity) : super(entity.key) {
        this.entity = Entity.builder(entity)
    }

    /**
     * Adds a property name and value to the entity to before updating.

     * @param name the property name.
     * *
     * @param value the property `Value`.
     * *
     * @return this update statement.
     */
    fun value(name: String, value: Any): Update {
        entity.property(name, value)
        return this
    }

    /**
     * Adds a property name and value to the entity before updating.

     * @param name the property name.
     * *
     * @param value the property `Value`.
     * *
     * @param indexed indicates whether the `Value` should be indexed or not.
     * *
     * @return this update statement.
     */
    fun value(name: String, value: Any, indexed: Boolean): Update {
        entity.property(name, value, indexed)
        return this
    }

    /**
     * Adds a property name and list of values to the entity before updating.

     * @param name the property name.
     * *
     * @param values a list of property `Value`s.
     * *
     * @return this update statement.
     */
    fun value(name: String, values: List<Any>): Update {
        entity.property(name, values)
        return this
    }

    /**
     * Adds a property name and list of values to the entity before updating.

     * @param name the property name.
     * *
     * @param values a list of property `Value`s.
     * *
     * @param indexed indicates whether the `Value`s should be indexed or not.
     * *
     * @return this update statement.
     */
    fun value(name: String, values: List<Any>, indexed: Boolean): Update {
        entity.property(name, values, indexed)
        return this
    }

    /**
     * Indicates whether this should be an "upsert" operation. An upsert will
     * add the enitiy if is does not exist, whereas a regualar update will fail
     * if the entity does not exist.

     * @return this update statement.
     */
    fun upsert(): Update {
        this.upsert = true
        return this
    }

    override fun getPb(namespace: String): Mutation {
        val mutation = com.google.datastore.v1.Mutation.newBuilder()
        if (upsert) {
            mutation.upsert = entity.build().getPb(namespace)
        } else {
            mutation.update = entity.build().getPb(namespace)
        }
        return mutation.build()
    }
}
