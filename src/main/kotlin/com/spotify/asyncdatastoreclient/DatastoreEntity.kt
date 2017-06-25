package com.spotify.asyncdatastoreclient

import com.google.datastore.v1.Entity
import com.google.datastore.v1.Value as PValue
import com.google.protobuf.ByteString
import java.util.*
import kotlin.reflect.KClass
import kotlin.reflect.KProperty
import kotlin.reflect.full.createInstance
import kotlin.reflect.full.isSubclassOf
import kotlin.reflect.full.primaryConstructor

@Suppress("UNCHECKED_CAST")
class DatastoreProperty<T : Any>(private val clazz: KClass<T>) {
    operator fun getValue(thisRef: DatastoreEntity, property: KProperty<*>): T? {
        val result = thisRef.entity.propertiesMap[property.name] ?: return null

        if (clazz == String::class.java) {
            return result.stringValue as T
        } else if (clazz == Long::class.java) {
            return result.integerValue as T
        } else if (clazz == Double::class.java) {
            return result.doubleValue as T
        } else if (clazz == Boolean::class.java) {
            return result.booleanValue as T
        } else if (clazz == Date::class.java) {
            return Date(result.timestampValue as Long) as T
        } else if (clazz == ByteString::class.java) {
            return result.blobValue as T
        } else if (clazz.isSubclassOf(DatastoreEntity::class)) {
            return clazz::primaryConstructor.call(result.entityValue) as T
        } else if (clazz == Key::class.java) {
            return result.keyValue as T
        } else {
            throw IllegalArgumentException("Unrecognised value type.")
        }

    }

    operator fun setValue(thisRef: DatastoreEntity, property: KProperty<*>, value: T?) {
        println("$value has been assigned to '${property.name} in $thisRef.'")
    }
}

class DatastoreStringProperty {
    operator fun getValue(thisRef: DatastoreEntity, property: KProperty<*>): String? {
        return thisRef.entity.propertiesMap[property.name]?.stringValue
    }

    operator fun setValue(thisRef: DatastoreEntity, property: KProperty<*>, value: String?) {
        val builder = PValue.newBuilder()
        builder.stringValue = value ?: ""
        thisRef.updates[property.name] = builder.build()
    }
}

fun string() = DatastoreStringProperty()

open class DatastoreEntity(internal val entity: Entity = Entity.getDefaultInstance()) {
    internal val updates = mutableMapOf<String, PValue>()

    fun pe(): Entity {
        return Entity.newBuilder(entity)
                .putAllProperties(updates)
                .build()
    }
}

class TestEntity(entity: Entity = Entity.getDefaultInstance()): DatastoreEntity(entity) {
    var name by string()
}

fun test() {
    with(TestEntity()) {
        name = "test123"
        pe()
    }
}
