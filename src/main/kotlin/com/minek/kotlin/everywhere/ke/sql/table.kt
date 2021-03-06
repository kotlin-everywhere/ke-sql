package com.minek.kotlin.everywhere.ke.sql

import io.reactiverse.pgclient.Row
import kotlin.reflect.KClass
import kotlin.reflect.KProperty1
import kotlin.reflect.full.companionObjectInstance
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
                    instance.tableInstanceMeta.state = TableInstance.State.Fetch
                    instance.tableInstanceMeta.map[it.name] = obj
                }
        instance.tableInstanceMeta.previousMap = instance.tableInstanceMeta.map.toMap()
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
    internal val createInstance = Class.forName(meta::class.jvmName.removeSuffix("\$Companion")).kotlin.primaryConstructor!!.apply { isAccessible = true }
    internal val primaryKey by lazy { columns.first { it.primaryKey } }
}

abstract class Table {
    @Suppress("LeakingThis")
    internal val tableInstanceMeta = TableInstance(this)
}

class TableInstance(private val table: Table) {
    val map = mutableMapOf<String, Any?>()
    var previousMap = mapOf<String, Any?>()
    var state = State.New
    val tableMeta by lazy {
        table::class.companionObjectInstance as TableMeta<*>
    }

    enum class State {
        New, Fetch, Delete
    }
}