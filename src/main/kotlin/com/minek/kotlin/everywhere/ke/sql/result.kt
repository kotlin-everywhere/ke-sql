package com.minek.kotlin.everywhere.ke.sql

import io.reactiverse.pgclient.Row

open class Result(private val row: Row, values: List<RowGetter<*>>) {
    private val fields = values
            .fold(0 to listOf<Any?>()) { (index, fields), rowGetter ->
                val (consumed, field) = rowGetter.get(row, index)
                (index + consumed) to (fields + field)
            }.second

    operator fun get(index: Int): Any? {
        return fields[index]
    }

    internal fun getTables(): List<Table> {
        return fields.filterIsInstance(Table::class.java)
    }
}

class Result1<T1>(row: Row, values: List<RowGetter<*>>) : Result(row, values) {
    @Suppress("UNCHECKED_CAST")
    operator fun component1(): T1 = get(0) as T1
}

interface RowGetter<T> {
    fun get(row: Row, index: Int): Pair<Int, T>
}