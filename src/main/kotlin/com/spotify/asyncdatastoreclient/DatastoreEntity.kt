package com.spotify.asyncdatastoreclient

import com.google.datastore.v1.Entity
import com.google.protobuf.ByteString
import java.util.*
import kotlin.reflect.KClass
import kotlin.reflect.KFunction2
import kotlin.reflect.KProperty
import kotlin.reflect.full.isSubclassOf
import kotlin.reflect.full.primaryConstructor
import com.google.datastore.v1.Value as PValue

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
        setter(property.name, thisRef.updates, PValue.Builder::setStringValue, value)
    }
}

class DatastoreLongProperty {
    operator fun getValue(thisRef: DatastoreEntity, property: KProperty<*>): Long? {
        return thisRef.entity.propertiesMap[property.name]?.integerValue
    }

    operator fun setValue(thisRef: DatastoreEntity, property: KProperty<*>, value: Long?) {
        setter(property.name, thisRef.updates, PValue.Builder::setIntegerValue, value)
    }
}

class DatastoreDoubleProperty {
    operator fun getValue(thisRef: DatastoreEntity, property: KProperty<*>): Double? {
        return thisRef.entity.propertiesMap[property.name]?.doubleValue
    }

    operator fun setValue(thisRef: DatastoreEntity, property: KProperty<*>, value: Double?) {
        setter(property.name, thisRef.updates, PValue.Builder::setDoubleValue, value)
    }
}

fun <T>setter(name: String, updates: MutableMap<String, Operation>, setMethod: KFunction2<PValue.Builder, T, PValue.Builder>, value: T?) {
    if (value == null) {
        updates[name] = DeleteOperation(name)
    } else {
        val builder = PValue.newBuilder()
        setMethod(builder, value)
        updates[name] = SetOperation(name, builder.build())
    }
}

class DatastoreBooleanProperty {
    operator fun getValue(thisRef: DatastoreEntity, property: KProperty<*>): Boolean? {
        return thisRef.entity.propertiesMap[property.name]?.booleanValue
    }

    operator fun setValue(thisRef: DatastoreEntity, property: KProperty<*>, value: Boolean?) {
        setter(property.name, thisRef.updates, PValue.Builder::setBooleanValue, value)
    }
}

class DatastoreDateProprety {
    operator fun getValue(thisRef: DatastoreEntity, property: KProperty<*>): Date? {
        val value = thisRef.entity.propertiesMap[property.name]?.timestampValue

        if (value != null) {
            return Date(value.seconds)
        } else {
            return null
        }
    }

    operator fun setValue(thisRef: DatastoreEntity, property: KProperty<*>, value: Boolean?) {
        setter(property.name, thisRef.updates, PValue.Builder::setBooleanValue, value)
    }
}

class DatastoreBlobProperty {
    operator fun getValue(thisRef: DatastoreEntity, property: KProperty<*>): ByteString? {
        return thisRef.entity.propertiesMap[property.name]?.stringValueBytes
    }

    operator fun setValue(thisRef: DatastoreEntity, property: KProperty<*>, value: ByteString?) {
        setter(property.name, thisRef.updates, PValue.Builder::setStringValueBytes, value)
    }
}

@Suppress("UNCHECKED_CAST")
class DatastoreEntityProperty<T: DatastoreEntity>(private val clazz: KClass<T>) {
    operator fun getValue(thisRef: DatastoreEntity, property: KProperty<*>): T? {
        val entity = thisRef.entity.propertiesMap[property.name]?.entityValue
        if (entity != null) {
            return clazz::primaryConstructor.call(entity) as T
        } else {
            return null
        }
    }

    operator fun setValue(thisRef: DatastoreEntity, property: KProperty<*>, value: T?) {
        if (value == null) {
            thisRef.updates[property.name] = DeleteOperation(property.name)
        } else {
            thisRef.updates[property.name] = SetPEntity(property.name, value)
        }
    }
}

interface Operation
data class DeleteOperation(val name: String): Operation
data class SetOperation(val name: String, val value: PValue): Operation
data class SetPEntity(val name: String, val value: HasPEntity): Operation

interface HasPEntity {
    fun pe(): Entity
}
open class DatastoreEntity(internal val entity: Entity = Entity.getDefaultInstance()): HasPEntity {
    internal val updates = mutableMapOf<String, Operation>()

    override fun pe(): Entity {
        return with(Entity.newBuilder(entity)) {
            for (update in updates.values) {
                when (update) {
                    is DeleteOperation -> removeProperties(update.name)
                    is SetOperation -> putProperties(update.name, update.value)
                    is SetPEntity -> {
                        putProperties(update.name, PValue
                                .newBuilder()
                                .setEntityValue(update.value.pe())
                                .build())
                    }
                }
            }
            build()
        }
    }

    fun string() = DatastoreStringProperty()
    fun long() = DatastoreLongProperty()
    fun double() = DatastoreDoubleProperty()
    fun boolean() = DatastoreBooleanProperty()
    fun date() = DatastoreDateProprety()
    fun blob() = DatastoreBlobProperty()
    inline fun <reified T: DatastoreEntity> entity() = DatastoreEntityProperty(T::class)
}

class TestEntity(entity: Entity = Entity.getDefaultInstance()): DatastoreEntity(entity) {
    var name by string()
    var number by long()
    val blob by entity<TestEntity>()
}

fun test() {
    val e = with(TestEntity()) {
        name = "test123"
        this
    }
}
