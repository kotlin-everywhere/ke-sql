package com.minek.kotlin.everywhere.ke.sql

import io.reactiverse.pgclient.Row

abstract class Type<T> : RowGetter<T>

open class IntType : Type<Int>() {
    override fun get(row: Row, index: Int): Pair<Int, Int> {
        return 1 to row.getInteger(index)
    }

    companion object : IntType()
}

open class StringType : Type<String>() {
    override fun get(row: Row, index: Int): Pair<Int, String> {
        return 1 to row.getString(index)
    }

    companion object : StringType()
}

