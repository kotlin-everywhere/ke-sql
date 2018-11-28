package com.minek.kotlin.everywhere.ke.sql

import java.sql.PreparedStatement
import java.sql.ResultSet
import java.sql.Statement

open class Result(private val row: ResultSet, values: List<RowGetter<*>>) {
    private val fields = values
            .fold(1 to listOf<Any?>()) { (index, fields), rowGetter ->
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

class Result1<T1>(row: ResultSet, values: List<RowGetter<*>>) : Result(row, values) {
    @Suppress("UNCHECKED_CAST")
    operator fun component1(): T1 = get(0) as T1
}

interface RowGetter<T> {
    fun get(row: ResultSet, index: Int): Pair<Int, T>
}

interface StatementSetter<T> {
    fun set(statement: PreparedStatement, index: Int, value: T)
}