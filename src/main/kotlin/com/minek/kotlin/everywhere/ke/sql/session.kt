package com.minek.kotlin.everywhere.ke.sql

import io.reactiverse.pgclient.PgPool
import io.reactiverse.pgclient.PgRowSet
import io.reactiverse.pgclient.Row
import io.reactiverse.pgclient.impl.ArrayTuple
import io.vertx.core.AsyncResult
import kotlinx.coroutines.experimental.CompletableDeferred


class Session(private val client: PgPool) {
    fun select(values: List<SelectValue<*>>): Select<Result> =
            select(values, ::Result)

    internal fun <T : Result> select(values: List<SelectValue<*>>, mapper: (Row, List<RowGetter<*>>) -> T) =
            Select(this, values, mapper)

    internal suspend fun preparedQuery(sql: String, arguments: List<Any?>): PgRowSet {
        val deferred = CompletableDeferred<AsyncResult<PgRowSet>>()
        client.preparedQuery(sql, ArrayTuple(arguments)) { deferred.complete(it) }
        val result = deferred.await()
        if (result.failed()) {
            throw result.cause()
        }
        return result.result()
    }
}

fun <T1> Session.select(value: SelectValue<T1>): Select<Result1<T1>> {
    return select(listOf(value), ::Result1)
}