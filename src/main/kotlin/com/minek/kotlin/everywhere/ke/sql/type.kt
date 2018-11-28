package com.minek.kotlin.everywhere.ke.sql

import java.sql.Connection
import java.sql.PreparedStatement
import java.sql.ResultSet

abstract class Type<T> : RowGetter<T>, StatementSetter<T> {
    abstract fun toArray(values: List<*>): Array<*>
    abstract fun arrayOf(connection: Connection, array: Array<Any?>): java.sql.Array
}

open class IntType : Type<Int>() {
    override fun arrayOf(connection: Connection, array: Array<Any?>): java.sql.Array {
        return connection.createArrayOf("int", array)
    }

    override fun set(statement: PreparedStatement, index: Int, value: Int) {
        statement.setInt(index, value)
    }

    override fun toArray(values: List<*>): Array<*> {
        return Array(values.size) { values[it] as Int }
    }

    override fun get(row: ResultSet, index: Int): Pair<Int, Int> {
        return 1 to row.getInt(index)
    }

    companion object : IntType()
}

open class StringType : Type<String>() {
    override fun arrayOf(connection: Connection, array: Array<Any?>): java.sql.Array {
        return connection.createArrayOf("string", array)
    }

    override fun set(statement: PreparedStatement, index: Int, value: String) {
        statement.setString(index, value)
    }

    override fun toArray(values: List<*>): Array<*> {
        return Array(values.size) { values[it] as String }
    }

    override fun get(row: ResultSet, index: Int): Pair<Int, String> {
        return 1 to row.getString(index)
    }

    companion object : StringType()
}

