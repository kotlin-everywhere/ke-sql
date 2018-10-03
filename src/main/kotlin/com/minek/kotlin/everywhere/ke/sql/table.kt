package com.minek.kotlin.everywhere.ke.sql

import io.reactiverse.pgclient.Row
import kotlin.reflect.KClass
import kotlin.reflect.KProperty1
import kotlin.reflect.full.isSubclassOf
import kotlin.reflect.full.memberProperties
import kotlin.reflect.full.primaryConstructor
import kotlin.reflect.jvm.isAccessible
import kotlin.reflect.jvm.jvmName

abstract class TableMeta<T : Table> : SelectValue<T>, FromValue {
    internal val meta by lazy { TableMetaData(this) }

    fun <T> column(type: Type<T>, name: String = "", primaryKey: Boolean = false, autoIncrement: Boolean = false, default: T? = null): Column<T> {
        return Column(this, type, name, primaryKey, autoIncrement, default)
    }

    override fun queryPair(index: Int): Pair<String, List<T>> {
        return meta.columns
                .joinToString(", ") { "${meta.name}.${it.name}" } to listOf()
    }

    override fun get(row: Row, index: Int): Pair<Int, T> {
        var consumed = 0
        val instance = meta.createInstance.call() as T
        meta
                .columns
                .forEach {
                    val (con, obj) = it.get(row, consumed + index)
                    consumed += con
                    instance.tableInstanceMeta.map[it.name] = obj
                }
        return consumed to instance
    }

    override fun fromQuery(): String {
        return meta.name
    }
}

internal class TableMetaData<T : TableMeta<*>>(meta: T) {
    internal val name = meta::class.qualifiedName!!.split(".").let { it[it.lastIndex - 1] }
    @Suppress("UNCHECKED_CAST")
    internal val columns =
            meta::class.memberProperties
                    .filter { (it.returnType.classifier as KClass<*>).isSubclassOf(Column::class) }
                    .map { it as KProperty1<Any, Any> }
                    .map { property ->
                        property.isAccessible = true
                        (property(meta) as Column<*>).apply {
                            if (name.isEmpty()) {
                                name = property.name
                            }
                        }
                    }
    internal val createInstance = Class.forName(meta::class.jvmName!!.removeSuffix("\$Companion")).kotlin.primaryConstructor!!.apply { isAccessible = true }
}

abstract class Table {
    internal val tableInstanceMeta = TableInstance()
}

class TableInstance {
    val map = mutableMapOf<String, Any?>()
}