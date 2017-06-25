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

import com.google.common.collect.Maps
import com.google.datastore.v1.Entity.Builder as PBuilder
import com.google.protobuf.ByteString
import com.google.datastore.v1.Entity as PEntity

import java.util.*

/**
 * Represents an entity that is stored in Datastore.

 * All properties are immutable; use `Entity.builder()` to construct new
 * `Entity` instances.
 */
class Entity private constructor(internal val pb: PEntity) {
    private val properties: Map<String, Value> by lazy {
        pb.propertiesMap.mapValues { Value.builder(it.value).build() }
    }

    class Builder(private val namespace: String, private val entity: PEntity.Builder, private val properties: MutableMap<String,Value> = mutableMapOf()) {

        /**
         * Creates a new `Entity`.

         * @return an immutable entity.
         */
        fun build(): Entity {
            entity.mutableProperties.clear()
            entity.putAllProperties(properties.mapValues { it.value.getPb(namespace).toBuilder().build() })
            return Entity(entity.build())
        }

        /**
         * Set the key for this entity.

         * @param key the key to set for this entity.
         * *
         * @return this entity builder.
         */
        fun key(key: Key): Builder {
            entity.key = key.getPb(namespace)
            return this
        }

        /**
         * Set property and its value for this entity.

         * @param name the property name to set.
         * *
         * @param value the property value.
         * *
         * @return this entity builder.
         */
        fun property(name: String, value: Value): Builder {
            properties.put(name, value)
            return this
        }

        /**
         * Set property and its value for this entity.

         * @param name the property name to set.
         * *
         * @param value the property value.
         * *
         * @return this entity builder.
         */
        fun property(name: String, value: Any): Builder {
            properties.put(name, Value.builder(value).build())
            return this
        }

        /**
         * Set property and its value for this entity.

         * @param name the property name to set.
         * *
         * @param value the property value.
         * *
         * @param indexed indicates whether the value should be indexed or not.
         * *
         * @return this entity builder.
         */
        fun property(name: String, value: Any, indexed: Boolean): Builder {
            properties.put(name, Value.builder(value).indexed(indexed).build())
            return this
        }

        /**
         * Set property and a list of value for this entity.

         * @param name the property name to set.
         * *
         * @param values a list of value.
         * *
         * @return this entity builder.
         */
        fun property(name: String, values: List<Any>): Builder {
            properties.put(name, Value.builder(values).build())
            return this
        }

        /**
         * Remove a property from this entity.

         * @param name the property name to remove.
         * *
         * @return this entity builder.
         */
        fun remove(name: String): Builder {
            properties.remove(name)
            return this
        }
    }

    /**
     * Return the key for this entity.

     * @return a `Key`.
     */
    val key: Key
        get() = Key.builder(pb.key).build()

    /**
     * Return the value for a given property as a string, or null
     * if the property doesn't exist.

     * @param name the name of the property to get.
     * *
     * @return a property value.
     */
    fun getString(name: String): String? {
        val value = properties[name]
        return value?.string
    }

    /**
     * Return the value for a given property as an integer, or null
     * if the property doesn't exist.

     * @param name the name of the property to get.
     * *
     * @return a property value.
     */
    fun getInteger(name: String): Long? {
        val value = properties[name]
        return value?.integer
    }

    /**
     * Return the value for a given property as a boolean, or null
     * if the property doesn't exist.

     * @param name the name of the property to get.
     * *
     * @return a property value.
     */
    fun getBoolean(name: String): Boolean? {
        val value = properties[name]
        return value?.boolean
    }

    /**
     * Return the value for a given property as a double, or null
     * if the property doesn't exist.

     * @param name the name of the property to get.
     * *
     * @return a property value.
     */
    fun getDouble(name: String): Double? {
        val value = properties[name]
        return value?.double
    }

    /**
     * Return the value for a given property as a date, or null
     * if the property doesn't exist.

     * @param name the name of the property to get.
     * *
     * @return a property value.
     */
    fun getDate(name: String): Date? {
        val value = properties[name]
        return value?.date
    }

    /**
     * Return the value for a given property as a blob, or null
     * if the property doesn't exist.

     * @param name the name of the property to get.
     * *
     * @return a property value.
     */
    fun getBlob(name: String): ByteString? {
        val value = properties[name]
        return value?.blob
    }

    /**
     * Return the value for a given property as an entity, or null
     * if the property doesn't exist.

     * @param name the name of the property to get.
     * *
     * @return a property value.
     */
    fun getEntity(name: String): Entity? {
        val value = properties[name]
        return value?.entity
    }

    /**
     * Return the value for a given property as a key, or null
     * if the property doesn't exist.

     * @param name the name of the property to get.
     * *
     * @return a property value.
     */
    fun getKey(name: String): Key? {
        val value = properties[name]
        return value?.key
    }

    /**
     * Return the value for a given property as a list of `Key`, or null
     * if the property doesn't exist.

     * @param name the name of the property to get.
     * *
     * @return a list of property values.
     */
    fun getList(name: String): List<Value>? {
        val value = properties[name]
        return value?.list
    }

    /**
     * Return the value for a given property as a list of values that are cast
     * to a given type, or null if the property doesn't exist.

     * @param name the name of the property to get.
     * *
     * @param clazz the type of class to cast values to.
     * *
     * @return a list of property values.
     */
    fun <T> getList(name: String, clazz: Class<T>): List<T>? {
        val value = properties[name]
        return value?.getList(clazz)
    }

    /**
     * Get the value for given property, or empty if none exists.

     * @param name name of the property to get.
     * *
     * @return an optional containing a property value, or empty.
     */
    operator fun get(name: String): Optional<Value> {
        return Optional.ofNullable(properties[name])
    }

    /**
     * Return whether a given property name exists in this entity.

     * @param name the name of the property.
     * *
     * @return true if the property exists.
     */
    operator fun contains(name: String): Boolean {
        return properties.containsKey(name)
    }

    override fun toString(): String = properties
            .asIterable()
            .map {"${it.key}:${it.value}"}
            .joinToString(",","{","}")

    override fun hashCode(): Int {
        return pb.hashCode()
    }

    override fun equals(other: Any?): Boolean {
        return other!!.javaClass == this.javaClass || other is Entity && pb == other.pb
    }

    internal fun getPb(namespace: String): PEntity {
        val propertiesLocal = pb.propertiesMap.mapValues { Value.builder(it.value).build().getPb(namespace) }

        return PEntity
                .newBuilder()
                .putAllProperties(propertiesLocal)
                .setKey(key.getPb(namespace))
                .build()
    }

    companion object {
        /**
         *
         *
         * Creates a new empty `Entity` builder.

         * @return an entity builder.
         */
        fun builder(namespace: String): Entity.Builder {
            return Entity.Builder(namespace, PEntity.newBuilder())
        }

        /**
         * Creates a new `Entity` builder for a given kind.

         * This is a shortcut for `Entity.builder().key(Key.builder(kind).build())`

         * @param kind the kind of entity.
         * *
         * @return an entity builder.
         */
        fun builder(namespace:String, kind: String): Entity.Builder {
            val key = Key.builder(kind).build().getPb(namespace)
            return Entity.Builder(namespace, PEntity.newBuilder().setKey(key))
        }

        /**
         * Creates a new `Entity` builder for a given kind and key id.

         * This is a shortcut for `Entity.builder().key(Key.builder(kind, id).build())`

         * @param kind the kind of entity.
         * *
         * @param id the key id.
         * *
         * @return an entity builder.
         */
        fun builder(namespace: String, kind: String, id: Long): Entity.Builder {
            val key = Key.builder(kind, id).build().getPb(namespace)
            return Entity.Builder(namespace, PEntity.newBuilder().setKey(key))
        }

        /**
         * Creates a new `Entity` builder for a given kind and key name.

         * This is a shortcut for `Entity.builder().key(Key.builder(kind, name).build())`

         * @param kind the kind of entity.
         * *
         * @param name the key name.
         * *
         * @return an entity builder.
         */
        fun builder(namespace: String, kind: String, name: String): Entity.Builder {
            val key = Key.builder(kind, name).build().getPb(namespace)
            return Entity.Builder(namespace, PEntity.newBuilder().setKey(key))
        }

        /**
         * Creates a new `Entity` builder for a given key.

         * This is a shortcut for `Entity.builder().key(key).build())`

         * @param key the key for this entity.
         * *
         * @return an entity builder.
         */
        fun builder(namespace: String, key: Key): Entity.Builder {
            return Entity.Builder(namespace, PEntity.newBuilder().setKey(key.getPb(namespace)))
        }

        /**
         * Creates a new `Entity` builder based on an existing entity.

         * @param entity the entity to use as a base.
         * *
         * @return an entity builder.
         */
        fun builder(namespace: String, entity: Entity): Entity.Builder {
            return Entity.Builder(namespace, PEntity.newBuilder(entity.pb))
        }

        internal fun builder(namespace: String, entity: PEntity): Entity.Builder {
            return Entity.Builder(namespace, PEntity.newBuilder(entity))
        }
    }
}
