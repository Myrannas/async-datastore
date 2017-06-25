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

import com.google.datastore.v1.PropertyReference
import com.google.datastore.v1.Filter as PFilter
import com.google.datastore.v1.PropertyFilter as PPropertyFilter

/**
 * Represents query filter.

 * A filter is composed of a property name, an operator and a value. Multiple
 * filters may be applied to a single `Query`.
 */
class Filter(private val name: String, private val op: Operator, private val value: Value) {

    enum class Operator {
        LESS_THAN,
        LESS_THAN_OR_EQUAL,
        GREATER_THAN,
        GREATER_THAN_OR_EQUAL,
        EQUAL,
        HAS_ANCESTOR
    }

    internal fun getPb(namespace: String): PFilter {
        val filter = with(PPropertyFilter.newBuilder()) {
            property = PropertyReference.newBuilder().setName(name).build()
            value = this@Filter.value.getPb(namespace)
            op = when (this@Filter.op) {
                Filter.Operator.LESS_THAN -> PPropertyFilter.Operator.LESS_THAN
                Filter.Operator.LESS_THAN_OR_EQUAL -> PPropertyFilter.Operator.LESS_THAN_OR_EQUAL
                Filter.Operator.GREATER_THAN -> PPropertyFilter.Operator.GREATER_THAN
                Filter.Operator.GREATER_THAN_OR_EQUAL -> PPropertyFilter.Operator.GREATER_THAN_OR_EQUAL
                Filter.Operator.EQUAL -> PPropertyFilter.Operator.EQUAL
                Filter.Operator.HAS_ANCESTOR -> PPropertyFilter.Operator.HAS_ANCESTOR
            }
            build()
        }
        return PFilter.newBuilder().setPropertyFilter(filter).build()
    }
}
