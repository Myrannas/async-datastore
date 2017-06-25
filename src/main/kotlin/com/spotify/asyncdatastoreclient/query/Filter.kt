package com.spotify.asyncdatastoreclient.query

import com.google.datastore.v1.PropertyFilter
import com.google.datastore.v1.PropertyFilter.*
import com.spotify.asyncdatastoreclient.Value
import com.google.datastore.v1.Filter as PFilter
/**
 * Created by michael on 25/06/17.
 */
class Filter(private val name: String, private val op: Filter.Operator, private val value: Value) {

    enum class Operator {
        LESS_THAN,
        LESS_THAN_OR_EQUAL,
        GREATER_THAN,
        GREATER_THAN_OR_EQUAL,
        EQUAL,
        HAS_ANCESTOR
    }

    internal fun getPb(namespace: String): PFilter {
        with(newBuilder()) {
            property = name
            value = this@Filter.value
            op = when (this@Filter.op) {
                Filter.Operator.LESS_THAN -> PropertyFilter.Operator.LESS_THAN
                Filter.Operator.LESS_THAN_OR_EQUAL -> PropertyFilter.Operator.LESS_THAN_OR_EQUAL
                Filter.Operator.GREATER_THAN -> PropertyFilter.Operator.GREATER_THAN
                Filter.Operator.GREATER_THAN_OR_EQUAL -> PropertyFilter.Operator.GREATER_THAN_OR_EQUAL
                Filter.Operator.EQUAL -> PropertyFilter.Operator.EQUAL
                Filter.Operator.HAS_ANCESTOR -> PropertyFilter.Operator.HAS_ANCESTOR
            }
            build()
        }
        return PFilter.build(filter)
    }
}