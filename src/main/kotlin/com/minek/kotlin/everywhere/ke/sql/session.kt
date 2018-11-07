package com.minek.kotlin.everywhere.ke.sql

import io.reactiverse.pgclient.PgPool
import io.reactiverse.pgclient.PgRowSet
import io.reactiverse.pgclient.Row
import io.reactiverse.pgclient.Tuple
import io.reactiverse.pgclient.impl.ArrayTuple
import io.vertx.core.AsyncResult
import kotlinx.coroutines.CompletableDeferred


class Session(private val client: PgPool) {
    private val tables = mutableListOf<Table>()

    fun select(values: List<SelectValue<*>>): Select<Result> =
            select(values, ::Result)

    fun add(value: Table) {
        tables.add(value)
    }

    suspend fun flush() {
        tables.asSequence()
                .filter { it.tableInstanceMeta.state == TableInstance.State.New }
                .groupBy { it.tableInstanceMeta.tableMeta }
                .forEach { (tableMeta, tables) ->
                    val tuples = tables.map { table ->
                        Tuple.tuple().apply {
                            tableMeta.meta.columns.forEach { column ->
                                addValue(table.tableInstanceMeta.map[column.name])
                            }
                        }

                    }
                    val deferred = CompletableDeferred<AsyncResult<PgRowSet>>()
                    val columns = tableMeta.meta.columns.joinToString() { it.name }
                    val values = (1..tableMeta.meta.columns.size).joinToString(",") { "\$$it" }
                    val insertQuery = "insert into ${tableMeta.meta.name} ($columns) values ($values)"
                    client.preparedBatch(insertQuery, tuples) { ar -> deferred.complete(ar) }
                    val result = deferred.await()
                    if (result.failed()) {
                        throw result.cause()
                    }
                }
    }

    fun clear() {
        tables.clear()
    }

    internal fun <T : Result> select(values: List<SelectValue<*>>, mapper: (Row, List<RowGetter<*>>) -> T) =
            Select(this, values, null, null, mapper)

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