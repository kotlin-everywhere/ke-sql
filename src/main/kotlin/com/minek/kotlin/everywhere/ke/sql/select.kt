package com.minek.kotlin.everywhere.ke.sql

import io.reactiverse.pgclient.PgRowSet
import io.reactiverse.pgclient.Row

class Select<T : Result>(
        private val session: Session,
        private val values: List<SelectValue<*>>,
        private val mapper: (Row, List<RowGetter<*>>) -> T
) {
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

    private suspend fun query(): PgRowSet {
        val (sqlList, sqlValues) = values.fold(listOf<String>() to listOf<Any?>()) { (sqlList, sqlValues), selectValue ->
            val (selectSql, selectValues) = selectValue.queryPair(sqlValues.size + 1)
            sqlList + selectSql to sqlValues + selectValues
        }
        return session.preparedQuery("select ${sqlList.joinToString(", ")}", sqlValues)
    }
}

interface SelectValue<T> : RowGetter<T> {
    fun queryPair(index: Int): Pair<String, List<T>>
}

class SelectResultSizeOverflowed : Exception()
class NoResultReturned : Exception()
