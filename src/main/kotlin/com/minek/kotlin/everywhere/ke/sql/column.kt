package com.minek.kotlin.everywhere.ke.sql

import io.reactiverse.pgclient.Row
import kotlin.reflect.KProperty

/**
 * Table Column Model
 */

class Column<T>(
        val tableMeta: TableMeta<*>,
        val type: Type<T>,
        name: String = "",
        val primaryKey: Boolean = false,
        val autoIncrement: Boolean = false,
        val default: T? = null
) : SelectValue<T>, OrderByValue {
    override fun queryPair(index: Int): Pair<String, List<T>> {
        return "${tableMeta.meta.name}.$name" to listOf()
    }

    override fun get(row: Row, index: Int): Pair<Int, T> {
        return type.get(row, index)
    }

    var name = name
        internal set

    operator fun getValue(table: Table, property: KProperty<*>): T {
        return table.tableInstanceMeta.map[property.name] as T
    }

    operator fun setValue(table: Table, property: KProperty<*>, value: T) {
        table.tableInstanceMeta.map[property.name] = value
    }

    override fun orderByQuery(index: Int): Pair<String, List<T>> {
        return queryPair(index)
    }
}