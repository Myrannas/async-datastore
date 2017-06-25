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
 * Represents an entity key.

 * A key is immutable; use `Key.builder()` to construct new
 * `Key` instances.
 */
class Key private constructor(internal val pb: com.google.datastore.v1.Key) {

    /**
     * Represents an key path element.

     * A key path element is immutable.
     */
    class Element internal constructor(internal val pb: com.google.datastore.v1.Key.PathElement) {

        val kind: String
            get() = pb.kind

        val id: Long?
            get() = if (isId(pb)) pb.id else null

        val name: String?
            get() = if (isName(pb)) pb.name else null

        override fun toString(): String {
            val id = id
            return kind + ":" + (id ?: name)
        }
    }

    class Builder {

        private val key: com.google.datastore.v1.Key.Builder

        internal constructor() {
            this.key = com.google.datastore.v1.Key.newBuilder()
        }

        private constructor(key: Key) : this(key.pb) {}

        private constructor(key: com.google.datastore.v1.Key) {
            this.key = com.google.datastore.v1.Key.newBuilder(key)
        }

        /**
         * Creates a new `Key`.

         * @return an immutable key.
         */
        fun build(): Key {
            return Key(key.build())
        }

        /**
         * Set the namespace for this `Key`.

         * @param namespace the namespace to set.
         * *
         * @return this key builder.
         */
        fun namespace(namespace: String): Builder {
            key.setPartitionId(
                    com.google.datastore.v1.PartitionId.newBuilder().setNamespaceId(namespace))
            return this
        }

        /**
         * Add a path element to this key.

         * @param element the path element to add.
         * *
         * @return this key builder.
         */
        fun path(element: Element): Builder {
            key.addPath(element.pb)
            return this
        }

        /**
         * Add a path element of a given kind to this key.

         * @param kind the path element kind.
         * *
         * @return this key builder.
         */
        fun path(kind: String): Builder {
            key.addPath(com.google.datastore.v1.Key.PathElement.newBuilder().setKind(kind))
            return this
        }

        /**
         * Add a path element of a given kind and id to this key.

         * @param kind the path element kind.
         * *
         * @param id the path element id.
         * *
         * @return this key builder.
         */
        fun path(kind: String, id: Long): Builder {
            key.addPath(com.google.datastore.v1.Key.PathElement.newBuilder().setKind(kind).setId(id))
            return this
        }

        /**
         * Add a path element of a given kind and name to this key.

         * @param kind the path element kind.
         * *
         * @param name the path element name.
         * *
         * @return this key builder.
         */
        fun path(kind: String, name: String): Builder {
            key.addPath(com.google.datastore.v1.Key.PathElement.newBuilder().setKind(kind).setName(name))
            return this
        }

        /**
         * Add a given key as a parent of this key.

         * @param parent the parent key to add.
         * *
         * @return this key builder.
         */
        fun parent(parent: Key): Builder {
            for (element in parent.path) {
                val id = element.id
                val name = element.name
                if (id != null) {
                    path(element.kind, id)
                } else if (name != null) {
                    path(element.kind, name)
                }
            }
            return this
        }
    }

    /**
     * Return the namespace for this key.

     * @return the namespace.
     */
    val namespace: String
        get() = pb.partitionId.namespaceId

    /**
     * Return whether this key is complete or not.

     * A complete key is a key that includes a "kind" and ("id" or "name").

     * @return true if the key is complete.
     */
    val isComplete: Boolean
        get() {
            if (pb.pathCount == 0) {
                return false
            }

            return pb.pathList.all { isId(it) || isName(it) }
        }

    /**
     * Return element kind of key, or null if not set.

     * This is a shortcut for `Key.getPath().get(Key.getPath().size() - 1).getKind()`

     * @return the element kind.
     */
    val kind: String?
        get() {
            val element = pb.pathList.lastOrNull() ?: return null
            return element.kind
        }

    /**
     * Return element key id, or null if not set.

     * This is a shortcut for `Key.getPath().get(Key.getPath().size() - 1).getId()`

     * @return the key id.
     */
    val id: Long?
        get() {
            val element = pb.pathList.lastOrNull() ?: return null
            return if (isId(element)) element.id else null
        }

    /**
     * Return element key name, or null if not set.

     * This is a shortcut for `Key.getPath().get(Key.getPath().size() - 1).getName()`

     * @return the key name.
     */
    val name: String?
        get() {
            val element = pb.pathList.lastOrNull() ?: return null
            return if (isName(element)) element.name else null
        }

    /**
     * Return element path that represents this key.

     * @return a list of path elements that make up this key.
     */
    val path: List<Element>
        get() = pb.pathList.map(::Element)

    override fun toString() = path.joinToString(",","{","}")

    override fun hashCode(): Int {
        return pb.hashCode()
    }

    override fun equals(other: Any?): Boolean {
        return other === this || other is Key && pb == other.pb
    }

    internal fun getPb(namespace: String?): com.google.datastore.v1.Key {
        if (namespace == null) {
            return pb
        } else {
            return com.google.datastore.v1.Key
                    .newBuilder(pb)
                    .setPartitionId(
                            com.google.datastore.v1.PartitionId
                                    .newBuilder().setNamespaceId(namespace)).build()
        }
    }

    companion object {

        /**
         * Creates a new empty `Key` builder.

         * @return an key builder.
         */
        fun builder(): Key.Builder {
            return Key.Builder()
        }

        /**
         * Creates a new `Key` builder for a given kind.

         * This is a shortcut for `Key.builder().path(kind).build()`

         * @param kind the kind of entity key.
         * *
         * @return a key builder.
         */
        fun builder(kind: String): Key.Builder {
            return Key.Builder().path(kind)
        }

        /**
         * Creates a new `Key` builder for a given kind with a parent.

         * This is a shortcut for `Key.builder().parent(parent).path(kind).build()`

         * @param kind the kind of entity key.
         * *
         * @param parent the parent key to add.
         * *
         * @return a key builder.
         */
        fun builder(kind: String, parent: Key): Key.Builder {
            return Key.Builder().parent(parent).path(kind)
        }

        /**
         * Creates a new `Key` builder for a given kind and an id.

         * This is a shortcut for `Key.builder().path(kind, id).build()`

         * @param kind the kind of entity key.
         * *
         * @param id the id of entity key.
         * *
         * @return a key builder.
         */
        fun builder(kind: String, id: Long): Key.Builder {
            return Key.Builder().path(kind, id)
        }

        /**
         * Creates a new `Key` builder for a given kind and an id with a parent.

         * This is a shortcut for `Key.builder().parent(parent).path(kind, id).build()`

         * @param kind the kind of entity key.
         * *
         * @param id the id of entity key.
         * *
         * @param parent the parent key to add.
         * *
         * @return a key builder.
         */
        fun builder(kind: String, id: Long, parent: Key): Key.Builder {
            return Key.Builder().parent(parent).path(kind, id)
        }

        /**
         * Creates a new `Key` builder for a given kind and a name.

         * This is a shortcut for `Key.builder().path(kind, name).build()`

         * @param kind the kind of entity key.
         * *
         * @param name the name of entity key.
         * *
         * @return a key builder.
         */
        fun builder(kind: String, name: String): Key.Builder {
            return Key.Builder().path(kind, name)
        }

        /**
         * Creates a new `Key` builder for a given kind and an name with a parent.

         * This is a shortcut for `Key.builder().parent(parent).path(kind, name).build()`

         * @param kind the kind of entity key.
         * *
         * @param name the name of entity key.
         * *
         * @param parent the parent key to add.
         * *
         * @return a key builder.
         */
        fun builder(kind: String, name: String, parent: Key): Key.Builder {
            return Key.Builder().parent(parent).path(kind, name)
        }

        /**
         * Creates a new `Key` builder based on an existing key.

         * @param key the key to base this builder.
         * *
         * @return a key builder.
         */
        fun builder(key: Key): Key.Builder {
            return Key.Builder(key)
        }

        internal fun builder(key: com.google.datastore.v1.Key): Key.Builder {
            return Key.Builder(key)
        }

        private fun isId(element: com.google.datastore.v1.Key.PathElement): Boolean {
            return element.idTypeCase == com.google.datastore.v1.Key.PathElement.IdTypeCase.ID
        }

        private fun isName(element: com.google.datastore.v1.Key.PathElement): Boolean {
            return element.idTypeCase == com.google.datastore.v1.Key.PathElement.IdTypeCase.NAME
        }
    }
}
