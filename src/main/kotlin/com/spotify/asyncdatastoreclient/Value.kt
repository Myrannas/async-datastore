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

import com.google.common.collect.ImmutableList
import com.google.datastore.v1.ArrayValue
import com.google.protobuf.ByteString
import com.google.protobuf.Timestamp
import com.google.datastore.v1.Value.ValueTypeCase
import com.google.datastore.v1.Entity as PEntity

import java.time.Instant
import java.util.Date
import java.util.stream.Collectors

/**
 * Represents an entity property value.

 * A value is immutable; use `Value.builder()` to construct new
 * `Value` instances.
 */
class Value private constructor(internal val pb: com.google.datastore.v1.Value) {

    class Builder {

        private val value: com.google.datastore.v1.Value.Builder

        private var excludeFromIndexes: Boolean = false

        internal constructor(builder: com.google.datastore.v1.Value.Builder) {
            this.value = builder
        }

        internal constructor() {
            this.value = com.google.datastore.v1.Value.newBuilder()
            this.excludeFromIndexes = false
        }

        internal constructor(value: Value) : this(value.pb)

        internal constructor(value: com.google.datastore.v1.Value) {
            this.value = com.google.datastore.v1.Value.newBuilder(value)
            this.excludeFromIndexes = value.excludeFromIndexes
        }

        /**
         * Creates a new `Value`.

         * @return an immutable value.
         */
        fun build(): Value {
            return Value(value.setExcludeFromIndexes(excludeFromIndexes).build())
        }

        /**
         * Set the value for this `Value`.
         *
         *
         * The supplied value must comply with the data types supported by Datastore.

         * @param value the value to set.
         * *
         * @return this value builder.
         * *
         * @throws IllegalArgumentException if supplied `value` is not recognised.
         * *
         */
        @Deprecated("Use type-specific builders instead, like {@code Value#fromString(String)}.")
        fun value(value: Any): Builder {
            if (value is String) {
                this.value.stringValue = value
            } else if (value is Boolean) {
                this.value.booleanValue = value
            } else if (value is Date) {
                this.value.timestampValue = toTimestamp(value)
            } else if (value is ByteString) {
                this.value.blobValue = value
            } else if (value is Entity) {
                this.value.setEntityValue(value.pb).excludeFromIndexes = true
            } else if (value is Key) {
                this.value.keyValue = value.pb
            } else if (value is Double) {
                this.value.doubleValue = value
            } else if (value is Long) {
                this.value.integerValue = value
            } else if (value is Float) {
                this.value.doubleValue = value.toDouble()
            } else if (value is Int) {
                this.value.integerValue = value.toLong()
            } else {
                throw IllegalArgumentException("Invalid value type: " + value.javaClass)
            }
            return this
        }

        /**
         * Set a list of values for this `Value`.
         *
         *
         * The supplied value items must comply with the data types supported by Datastore.

         * @param values a list of values to set.
         * *
         * @return this value builder.
         * *
         * @throws IllegalArgumentException if supplied `values` contains types
         * * that are not recognised.
         */
        fun value(values: List<Any>): Builder {
            val arrayValues = ArrayValue
                    .newBuilder()
                    .addAllValues(values.map { value -> Value.builder(value).build().pb })
                    .build()

            this.value.arrayValue = arrayValues

            return this
        }

        /**
         * Set a whether this value should be indexed or not.

         * @param indexed indicates whether value is indexed.
         * *
         * @return this value builder.
         */
        fun indexed(indexed: Boolean): Builder {
            this.excludeFromIndexes = !indexed
            return this
        }

    }

    /**
     * Return the value as a string.

     * @return the value.
     * *
     * @throws IllegalArgumentException if `Value` is not a string.
     */
    val string: String
        get() {
            if (!isString) {
                throw IllegalArgumentException("Value does not contain a string.")
            }
            return pb.stringValue
        }

    val isString: Boolean
        get() = pb.valueTypeCase == com.google.datastore.v1.Value.ValueTypeCase.STRING_VALUE

    /**
     * Return the value as an integer.

     * @return the value.
     * *
     * @throws IllegalArgumentException if `Value` is not an integer.
     */
    val integer: Long
        get() {
            if (!isInteger) {
                throw IllegalArgumentException("Value does not contain an integer.")
            }
            return pb.integerValue
        }

    /**
     * Check if value is a integer.

     * @return `true` if value is a integer.
     */
    val isInteger: Boolean
        get() = pb.valueTypeCase == com.google.datastore.v1.Value.ValueTypeCase.INTEGER_VALUE

    /**
     * Return the value as a boolean.

     * @return the value.
     * *
     * @throws IllegalArgumentException if `Value` is not a boolean.
     */
    val boolean: Boolean
        get() {
            if (!isBoolean) {
                throw IllegalArgumentException("Value does not contain a boolean.")
            }
            return pb.booleanValue
        }

    /**
     * Check if value is a boolean.

     * @return `true` if value is a boolean.
     */
    val isBoolean: Boolean
        get() = pb.valueTypeCase == com.google.datastore.v1.Value.ValueTypeCase.BOOLEAN_VALUE

    /**
     * Return the value as a double.

     * @return the value.
     * *
     * @throws IllegalArgumentException if `Value` is not a double.
     */
    val double: Double
        get() {
            if (!isDouble) {
                throw IllegalArgumentException("Value does not contain a double.")
            }
            return pb.doubleValue
        }

    /**
     * Check if value is a double.

     * @return `true` if value is a double.
     */
    val isDouble: Boolean
        get() = pb.valueTypeCase == com.google.datastore.v1.Value.ValueTypeCase.DOUBLE_VALUE

    /**
     * Return the value as a date.

     * @return the value.
     * *
     * @throws IllegalArgumentException if `Value` is not a date.
     */
    val date: Date
        get() {
            if (!isDate) {
                throw IllegalArgumentException("Value does not contain a timestamp.")
            }

            return toDate(pb.timestampValue)
        }

    /**
     * Check if value is a date.

     * @return `true` if value is a date.
     */
    val isDate: Boolean
        get() = pb.valueTypeCase == com.google.datastore.v1.Value.ValueTypeCase.TIMESTAMP_VALUE

    /**
     * Return the value as a blob.

     * @return the value.
     * *
     * @throws IllegalArgumentException if `Value` is not a blob.
     */
    val blob: ByteString
        get() {
            if (!isBlob) {
                throw IllegalArgumentException("Value does not contain a blob.")
            }
            return pb.blobValue
        }

    /**
     * Check if value is a blob.

     * @return `true` if value is a blob.
     */
    val isBlob: Boolean
        get() = pb.valueTypeCase == com.google.datastore.v1.Value.ValueTypeCase.BLOB_VALUE

    /**
     * Return the value as an `Entity`.

     * @return the value.
     * *
     * @throws IllegalArgumentException if `Value` is not an entity.
     */
    val entity: Entity
        get() {
            if (!isEntity) {
                throw IllegalArgumentException("Value does not contain an entity.")
            }
            TODO()
            return Entity.builder("", pb.entityValue).build()
        }

    /**
     * Check if value is a entity.

     * @return `true` if value is a entity.
     */
    val isEntity: Boolean
        get() = pb.valueTypeCase == com.google.datastore.v1.Value.ValueTypeCase.ENTITY_VALUE

    /**
     * Return the value as a `Key`.

     * @return the value.
     * *
     * @throws IllegalArgumentException if `Value` is not a key.
     */
    val key: Key
        get() {
            val key = pb.keyValue ?: throw IllegalArgumentException("Value does not contain an key.")
            return Key.builder(key).build()
        }

    /**
     * Check if value is a key.

     * @return `true` if value is a key.
     */
    val isKey: Boolean
        get() = pb.keyValue != null

    /**
     * Return the value as a list of `Value`.

     * @return the list value, an empty list indicates that the value is not set.
     */
    val list: List<Value>
        get() = pb.arrayValue.valuesList.map { Value(it) }

    /**
     * Check if value is a list.
     * Empty lists are not detected as lists.

     * @return `true` if value is a list.
     */
    val isList: Boolean
        get() = pb.valueTypeCase == com.google.datastore.v1.Value.ValueTypeCase.ARRAY_VALUE

    /**
     * Return the value as a list of objects cast to a given type.

     * @param clazz the type of class to cast values to.
     * *
     * @return the value.
     * *
     * @throws IllegalArgumentException if `Value` is not a list.
     */
    fun <T> getList(clazz: Class<T>): List<T> {
        return list.map { valueLocal -> valueLocal.convert(clazz) }
    }

    internal fun <T> convert(clazz: Class<T>): T {
        if (clazz == String::class.java) {
            return string as T
        } else if (clazz == Long::class.java) {
            return java.lang.Long.valueOf(integer) as T
        } else if (clazz == Double::class.java) {
            return java.lang.Double.valueOf(double) as T
        } else if (clazz == Boolean::class.java) {
            return java.lang.Boolean.valueOf(boolean) as T
        } else if (clazz == Date::class.java) {
            return date as T
        } else if (clazz == ByteString::class.java) {
            return blob as T
        } else if (clazz == Entity::class.java) {
            return entity as T
        } else if (clazz == Key::class.java) {
            return key as T
        } else {
            throw IllegalArgumentException("Unrecognised value type.")
        }
    }

    /**
     * Returns whether the value is indexed or not.

     * @return true if the value is indexed.
     */
    val isIndexed: Boolean
        get() = !pb.excludeFromIndexes

    internal fun getPb(namespace: String): com.google.datastore.v1.Value {
        val key = pb.keyValue
        if (key.pathCount > 0) {
            return com.google.datastore.v1.Value.newBuilder(pb)
                    .setKeyValue(key.getPb(namespace)).build()
        }
        return pb
    }

    override fun hashCode(): Int {
        return pb.hashCode()
    }

    override fun equals(other: Any?): Boolean {
        return other!!.javaClass == this.javaClass && other is Value && pb == other.pb
    }

    override fun toString(): String = when (pb.valueTypeCase) {
        ValueTypeCase.KEY_VALUE -> key.toString()
        ValueTypeCase.STRING_VALUE -> pb.stringValue
        ValueTypeCase.BLOB_VALUE -> "<binary>"
        ValueTypeCase.TIMESTAMP_VALUE -> date.toString()
        ValueTypeCase.INTEGER_VALUE -> pb.integerValue.toString()
        ValueTypeCase.DOUBLE_VALUE -> pb.doubleValue.toString()
        ValueTypeCase.BOOLEAN_VALUE -> pb.booleanValue.toString()
        ValueTypeCase.ENTITY_VALUE -> entity.toString()
        ValueTypeCase.ARRAY_VALUE -> list.joinToString(",", "[", "]")
        ValueTypeCase.NULL_VALUE -> "<null>"
        else -> pb.toString()
    }

    private fun toDate(timestamp: Timestamp): Date {
        return Date.from(Instant.ofEpochSecond(timestamp.seconds, timestamp.nanos.toLong()))
    }

    companion object {

        private fun toTimestamp(date: Date): Timestamp {
            val millis = date.time
            return Timestamp
                    .newBuilder()
                    .setSeconds(millis / 1000)
                    .setNanos((millis % 1000 * 1000000).toInt())
                    .build()
        }

        /**
         * Creates a new empty `Value` builder.

         * @return an value builder.
         */
        fun builder(): Value.Builder {
            return Value.Builder()
        }

        /**
         * Create a new value containing a string.

         * @param value The string to build a value from.
         * *
         * @return A new value builder.
         */
        fun from(value: String): Value.Builder {
            return Value.Builder(com.google.datastore.v1.Value.newBuilder().setStringValue(value))
        }

        /**
         * Create a new value containing a boolean.

         * @param value The boolean to build a value from.
         * *
         * @return A new value builder.
         */
        fun from(value: Boolean): Value.Builder {
            return Value.Builder(com.google.datastore.v1.Value.newBuilder().setBooleanValue(value))
        }

        /**
         * Create a new value containing a date.

         * @param value The date to build a value from.
         * *
         * @return A new value builder.
         */
        fun from(value: Date): Value.Builder {
            return Value.Builder(com.google.datastore.v1.Value.newBuilder().setTimestampValue(toTimestamp(value)))
        }

        /**
         * Create a new value containing a blob.

         * @param value The blob to build a value from.
         * *
         * @return A new value builder.
         */
        fun from(value: ByteString): Value.Builder {
            return Value.Builder(com.google.datastore.v1.Value.newBuilder().setBlobValue(value))
        }

        /**
         * Create a new value containing a entity.

         * @param entity The entity to build a value from.
         * *
         * @return A new value builder.
         */
        fun from(entity: Entity): Value.Builder {
            return Value.Builder(com.google.datastore.v1.Value.newBuilder().setEntityValue(entity.pb))
        }

        /**
         * Create a new value containing a key.

         * @param key The key to build a value from.
         * *
         * @return A new value builder.
         */
        fun from(key: Key): Value.Builder {
            return Value.Builder(com.google.datastore.v1.Value.newBuilder().setKeyValue(key.pb))
        }

        /**
         * Create a new value containing a double.

         * @param value The double to build a value from.
         * *
         * @return A new value builder.
         */
        fun from(value: Double): Value.Builder {
            return Value.Builder(com.google.datastore.v1.Value.newBuilder().setDoubleValue(value))
        }

        /**
         * Create a new value containing a float.

         * @param value The float to build a value from.
         * *
         * @return A new value builder.
         */
        fun from(value: Float): Value.Builder {
            return Value.Builder(com.google.datastore.v1.Value.newBuilder().setDoubleValue(value.toDouble()))
        }

        /**
         * Create a new value containing a integer.

         * @param value The integer to build a value from.
         * *
         * @return A new value builder.
         */
        fun from(value: Int): Value.Builder {
            return Value.Builder(com.google.datastore.v1.Value.newBuilder().setIntegerValue(value.toLong()))
        }

        /**
         * Create a new value containing a long.

         * @param value The long to build a value from.
         * *
         * @return A new value builder.
         */
        fun from(value: Long): Value.Builder {
            return Value.Builder(com.google.datastore.v1.Value.newBuilder().setIntegerValue(value))
        }

        /**
         * Create a new value containing an existing value.

         * @param value Values to add to builder.
         * *
         * @return A new value builder containing a list.
         */
        fun from(value: Value): Value.Builder {
            return Value.Builder(com.google.datastore.v1.Value.newBuilder(value.pb))
        }

        /**
         * Create a new value containing a list of values.

         * @param values List of values to add to builder.
         * *
         * @return A new value builder containing a list.
         */
        fun from(values: List<Value>): Value.Builder {
            val builder = com.google.datastore.v1.ArrayValue.newBuilder()

            values.stream().forEach { v -> builder.addValues(v.pb) }

            return Value.Builder(com.google.datastore.v1.Value.newBuilder().setArrayValue(builder).build())
        }

        /**
         * Create a new value containing an array of values.

         * @param first First value to add.
         * *
         * @param values Array of values to add to builder.
         * *
         * @return A new value builder containing a list.
         */
        fun from(first: Value, vararg values: Value): Value.Builder {
            val builder = com.google.datastore.v1.ArrayValue.newBuilder()

            builder.addValues(first.pb)
            for (v in values) {
                builder.addValues(v.pb)
            }

            return Value.Builder(
                    com.google.datastore.v1.Value.newBuilder().setArrayValue(builder).build())
        }

        /**
         * Creates a new `Value` builder based on an existing value.

         * @param value the value to use as a base.
         * *
         * @return an value builder.
         * *
         */
        @Deprecated("Use {@link #from(Value)}.")
        fun builder(value: Value): Value.Builder {
            return Value.Builder(value)
        }

        /**
         * Creates a new `Value` builder based on a given value.

         * @param value the value to set.
         * *
         * @return an value builder.
         * *
         */
        @Deprecated("Prefer value-specific builders, like {@link #from(String)}.")
        fun builder(value: Any): Value.Builder {
            return Builder().value(value)
        }

        /**
         * Creates a new `Value` builder based on a given list of values.

         * @param values the list of values to set.
         * *
         * @return an value builder.
         */
        fun builder(values: List<Any>): Value.Builder {
            return Builder().value(values)
        }

        internal fun builder(value: com.google.datastore.v1.Value): Value.Builder {
            return Value.Builder(value)
        }
    }
}
