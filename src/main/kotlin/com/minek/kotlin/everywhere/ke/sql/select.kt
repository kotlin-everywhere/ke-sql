package com.minek.kotlin.everywhere.ke.sql

import io.reactiverse.pgclient.Row

class Select<T : Result>(
        private val session: Session,
        private val values: List<SelectValue<*>>,
        private val mapper: (Row, List<RowGetter<*>>) -> T
) {
    suspend fun maybeOne(): T? {
        val (sqlList, sqlValues) = values.fold(listOf<String>() to listOf<Any?>()) { (sqlList, sqlValues), selectValue ->
            val (selectSql, selectValues) = selectValue.queryPair(sqlValues.size + 1)
            sqlList + selectSql to sqlValues + selectValues
        }
        val pgRowSet = session.preparedQuery("select ${sqlList.joinToString(", ")}", sqlValues)
        val row = pgRowSet.firstOrNull() ?: return null
        return mapper(row, values)
    }
}

interface SelectValue<T> : RowGetter<T> {
    fun queryPair(index: Int): Pair<String, List<T>>
}