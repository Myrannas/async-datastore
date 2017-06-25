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
 * An insert statement.

 * Insert a single entity using a given `Key` and properties.
 */
class Insert : KeyedStatement, MutationStatement {

    protected var entity: Entity.Builder

    internal constructor(key: Key) : super(key) {
        this.entity = Entity.builder(key)
    }

    internal constructor(entity: Entity) : super(entity.key) {
        this.entity = Entity.builder(entity)
    }

    /**
     * Adds a property name and value to the entity to before inserting.

     * @param name the property name.
     * *
     * @param value the property `Value`.
     * *
     * @return this insert statement.
     */
    fun value(name: String, value: Any): Insert {
        entity.property(name, value)
        return this
    }

    /**
     * Adds a property name and value to the entity before inserting.

     * @param name the property name.
     * *
     * @param value the property `Value`.
     * *
     * @param indexed indicates whether the `Value` should be indexed or not.
     * *
     * @return this insert statement.
     */
    fun value(name: String, value: Any, indexed: Boolean): Insert {
        entity.property(name, value, indexed)
        return this
    }

    /**
     * Adds a property name and list of values to the entity before inserting.

     * @param name the property name.
     * *
     * @param values a list of property `Value`s.
     * *
     * @return this insert statement.
     */
    fun value(name: String, values: List<Any>): Insert {
        entity.property(name, values)
        return this
    }

    override fun getPb(namespace: String): Mutation {
        val mutation = com.google.datastore.v1.Mutation.newBuilder()

        return mutation.setInsert(entity.build().getPb(namespace)).build()
    }
}
