package com.minek.kotlin.everywhere.ke.sql

import java.sql.ResultSet

data class Select<T : Result>(
        private val session: Session,
        private val values: List<SelectValue<*>>,
        private val from: FromValue? = null,
        private val orderBy: OrderByValue? = null,
        private val mapper: (ResultSet, List<RowGetter<*>>) -> T
) {
    fun from(from: FromValue): Select<T> {
        return copy(from = from)
    }

    fun orderBy(orderBy: OrderByValue): Select<T> {
        return copy(orderBy = orderBy)
    }

    suspend fun maybeOne(): T? {
        return query { resultSet ->
            if (!resultSet.next()) {
                null
            } else {
                val row = mapper(resultSet, values)
                if (resultSet.next()) {
                    throw SelectResultSizeOverflowed()
                }
                row
            }
        }
    }

    suspend fun one(): T {
        return query { resultSet ->
            if (!resultSet.next()) {
                throw NoResultReturned()
            }
            val row = mapper(resultSet, values)
            if (resultSet.next()) {
                throw SelectResultSizeOverflowed()
            }
            row
        }
    }

    suspend fun all(): List<T> {
        return query { resultSet ->
            val rows = mutableListOf<T>()
            while (resultSet.next()) {
                rows.add(mapper(resultSet, values))
            }
            rows
        }
    }

    private suspend fun <T> query(resultMapper: (ResultSet) -> T): T {
        val (sqlList, sqlValues) = values.fold(listOf<String>() to listOf<Pair<StatementSetter<Any?>, Any?>>()) { (sqlList, sqlValues), selectValue ->
            val (selectSql, selectValues) = selectValue.queryPair(sqlValues.size + 1) ?: ("null" to listOf())
            sqlList + selectSql to sqlValues + selectValues
        }
        val select = "select ${sqlList.joinToString(", ")}"
        val from = if (from != null) "from ${from.fromQuery()}" else null
        val orderBy = if (orderBy != null) "order by ${orderBy.orderByQuery(sqlValues.size + 1).first}" else null
        return session.preparedQuery(listOfNotNull(select, from, orderBy).joinToString(" "), sqlValues, resultMapper)
    }
}

interface SelectValue<T> : Value<T>, RowGetter<T>

interface FromValue {
    fun fromQuery(): String
}

class SelectResultSizeOverflowed : Exception()
class NoResultReturned : Exception()
