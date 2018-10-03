package com.minek.kotlin.everywhere.ke.sql

import io.reactiverse.pgclient.Row

open class Result(private val row: Row, private val values: List<RowGetter<*>>) {
    operator fun get(index: Int): Any? {
        return values[index].get(row, index).second
    }
}

class Result1<T1>(row: Row, values: List<RowGetter<*>>) : Result(row, values) {
    @Suppress("UNCHECKED_CAST")
    operator fun component1(): T1 = get(0) as T1
}

interface RowGetter<T> {
    fun get(row: Row, index: Int): Pair<Int, T>
}