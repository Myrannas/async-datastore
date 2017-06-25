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

import com.google.datastore.v1.PropertyOrder
import com.google.datastore.v1.PropertyOrder.Direction.ASCENDING
import com.google.datastore.v1.PropertyOrder.Direction.DESCENDING
import com.google.datastore.v1.PropertyReference

/**
 * Represents query order.

 * An order is composed of a property name and a direction. Multiple orders
 * may be applied to a single `Query`.
 */
class Order internal constructor(private val name: String, private val dir: Direction) {

    enum class Direction {
        ASCENDING,
        DESCENDING
    }

    internal val pb: PropertyOrder
        get() = with(PropertyOrder.newBuilder()) {
            property = PropertyReference.newBuilder().setName(name).build()
            direction = if (dir == Direction.ASCENDING) ASCENDING else DESCENDING
            build()
        }
}
