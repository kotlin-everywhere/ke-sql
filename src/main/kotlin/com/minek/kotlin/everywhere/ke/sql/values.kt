package com.minek.kotlin.everywhere.ke.sql

import io.reactiverse.pgclient.Row

abstract class Value<T> : SelectValue<T>

data class IntValue(private val int: Int) : Value<Int>() {
    override fun get(row: Row, index: Int): Pair<Int, Int> = 1 to row.getInteger(index)

    override fun queryPair(index: Int): Pair<String, List<Int>> {
        return "\$$index::int" to listOf(int)
    }
}

val Int.v: Value<Int>
    get() = IntValue(this)


sealed class Expression<T> : Value<T>()

data class Plus<T>(val left: Value<T>, val right: Value<T>) : Expression<T>() {
    override fun get(row: Row, index: Int): Pair<Int, T> = left.get(row, index)

    override fun queryPair(index: Int): Pair<String, List<T>> {
        val (leftSql, leftValues) = left.queryPair(index)
        val (rightSql, rightValues) = right.queryPair(index + leftValues.size)
        return "($leftSql) + ($rightSql)" to (leftValues + rightValues)
    }
}

infix fun <T> Value<T>.plus(right: Value<T>): Value<T> {
    return Plus(this, right)
}
