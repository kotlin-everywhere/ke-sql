package com.minek.kotlin.everywhere.ke.sql

import io.reactiverse.pgclient.PgRowSet
import io.reactiverse.pgclient.Row

data class Select<T : Result>(
        private val session: Session,
        private val values: List<SelectValue<*>>,
        private val from: FromValue? = null,
        private val orderBy: OrderByValue? = null,
        private val mapper: (Row, List<RowGetter<*>>) -> T
) {
    fun from(from: FromValue): Select<T> {
        return copy(from = from)
    }

    fun orderBy(orderBy: OrderByValue): Select<T> {
        return copy(orderBy = orderBy)
    }

    suspend fun maybeOne(): T? {
        val pgRowSet = query()
        if (pgRowSet.size() > 1) {
            throw SelectResultSizeOverflowed()
        }
        val row = pgRowSet.firstOrNull() ?: return null
        return mapper(row, values)
    }

    suspend fun one(): T {
        val pgRowSet = query()
        val row = pgRowSet.firstOrNull() ?: throw NoResultReturned()
        return mapper(row, values)
    }

    suspend fun all(): Sequence<T> {
        return query().asSequence().map { mapper(it, values) }
    }

    private suspend fun query(): PgRowSet {
        val (sqlList, sqlValues) = values.fold(listOf<String>() to listOf<Any?>()) { (sqlList, sqlValues), selectValue ->
            val (selectSql, selectValues) = selectValue.queryPair(sqlValues.size + 1) ?: ("null" to listOf())
            sqlList + selectSql to sqlValues + selectValues
        }
        val select = "select ${sqlList.joinToString(", ")}"
        val from = if (from != null) "from ${from.fromQuery()}" else null
        val orderBy = if (orderBy != null) "order by ${orderBy.orderByQuery(sqlValues.size + 1).first}" else null
        return session.preparedQuery(listOfNotNull(select, from, orderBy).joinToString(" "), sqlValues)
    }
}

interface SelectValue<T> : Value<T>, RowGetter<T>

interface FromValue {
    fun fromQuery(): String
}

class SelectResultSizeOverflowed : Exception()
class NoResultReturned : Exception()
