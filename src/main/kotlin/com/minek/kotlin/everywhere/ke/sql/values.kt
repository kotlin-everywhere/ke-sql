package com.minek.kotlin.everywhere.ke.sql

import java.sql.PreparedStatement
import java.sql.ResultSet

interface QueryPair {
    fun queryPair(index: Int): Pair<String, List<Pair<StatementSetter<Any?>, Any?>>>?
}

interface Value<T> : QueryPair, StatementSetter<T>

data class IntValue(private val int: Int) : SelectValue<Int> {
    override fun set(statement: PreparedStatement, index: Int, value: Int) {
        statement.setInt(index, value)
    }

    override fun get(row: ResultSet, index: Int): Pair<Int, Int> = 1 to row.getInt(index)

    override fun queryPair(index: Int):  Pair<String, List<Pair<StatementSetter<Any?>, Any?>>>?{
        return "?" to listOf(this as StatementSetter<Any?> to int)
    }
}

val Int.v: IntValue
    get() = IntValue(this)


sealed class Expression<T> : Value<T>

data class Plus<T>(val left: SelectValue<T>, val right: SelectValue<T>) : SelectValue<T>, Expression<T>() {
    override fun set(statement: PreparedStatement, index: Int, value: T) {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun get(row: ResultSet, index: Int): Pair<Int, T> = left.get(row, index)

    override fun queryPair(index: Int):  Pair<String, List<Pair<StatementSetter<Any?>, Any?>>>? {
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
    override fun set(statement: PreparedStatement, index: Int, value: Boolean) {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun queryPair(index: Int):  Pair<String, List<Pair<StatementSetter<Any?>, Any?>>>? {
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

    override fun get(row: ResultSet, index: Int): Pair<Int, Boolean> {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }
}

infix fun <T> Value<T>.eq(right: Value<T>): Eq<T> {
    return Eq(this, right)
}


data class And<T>(val left: Value<T>, val right: Value<T>) : SelectValue<Boolean>, Expression<Boolean>(), Condition {
    override fun set(statement: PreparedStatement, index: Int, value: Boolean) {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun queryPair(index: Int):  Pair<String, List<Pair<StatementSetter<Any?>, Any?>>>? {
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

    override fun get(row: ResultSet, index: Int): Pair<Int, Boolean> {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }
}

infix fun <T> Value<T>.and(right: Value<T>): And<T> {
    return And(this, right)
}
