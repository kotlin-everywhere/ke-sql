package com.minek.kotlin.everywhere.ke.sql

import io.reactiverse.pgclient.Row

interface QueryPair {
    fun queryPair(index: Int): Pair<String, List<Any?>>?
}

interface Value<T> : QueryPair

data class IntValue(private val int: Int) : SelectValue<Int> {
    override fun get(row: Row, index: Int): Pair<Int, Int> = 1 to row.getInteger(index)

    override fun queryPair(index: Int): Pair<String, List<Int>> {
        return "\$$index::int" to listOf(int)
    }
}

val Int.v: IntValue
    get() = IntValue(this)


sealed class Expression<T> : Value<T>

data class Plus<T>(val left: SelectValue<T>, val right: SelectValue<T>) : SelectValue<T>, Expression<T>() {
    override fun get(row: Row, index: Int): Pair<Int, T> = left.get(row, index)

    override fun queryPair(index: Int): Pair<String, List<Any?>>? {
        val leftSql = left.queryPair(index)
        val rightSql = right.queryPair(index + (leftSql?.second?.size ?: 0))
        if (leftSql == null) {
            return rightSql
        }
        if (rightSql == null) {
            return null
        }
        return "(${leftSql.first}) + (${rightSql.first})" to (leftSql.second + rightSql.second)
    }
}

infix fun <T> SelectValue<T>.plus(right: SelectValue<T>): Plus<T> {
    return Plus(this, right)
}

data class Eq<T>(val left: Value<T>, val right: Value<T>) : SelectValue<Boolean>, Expression<Boolean>(), Condition {
    override fun queryPair(index: Int): Pair<String, List<Any?>>? {
        val leftSql = left.queryPair(index)
        val rightSql = right.queryPair(index + (leftSql?.second?.size ?: 0))
        if (leftSql == null) {
            return rightSql
        }
        if (rightSql == null) {
            return null
        }
        return "(${leftSql.first}) = (${rightSql.first})" to (leftSql.second + rightSql.second)
    }

    override fun get(row: Row, index: Int): Pair<Int, Boolean> {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }
}

infix fun <T> Value<T>.eq(right: Value<T>): Eq<T> {
    return Eq(this, right)
}


data class And<T>(val left: Value<T>, val right: Value<T>) : SelectValue<Boolean>, Expression<Boolean>(), Condition {
    override fun queryPair(index: Int): Pair<String, List<Any?>>? {
        val leftSql = left.queryPair(index)
        val rightSql = right.queryPair(index + (leftSql?.second?.size ?: 0))
        if (leftSql == null) {
            return rightSql
        }
        if (rightSql == null) {
            return null
        }
        return "(${leftSql.first}) and (${rightSql.first})" to (leftSql.second + rightSql.second)
    }

    override fun get(row: Row, index: Int): Pair<Int, Boolean> {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }
}

infix fun <T> Value<T>.and(right: Value<T>): And<T> {
    return And(this, right)
}
