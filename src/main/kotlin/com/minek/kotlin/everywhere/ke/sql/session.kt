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

    fun remove(value: Table) {
        value.tableInstanceMeta.state = TableInstance.State.Delete
    }

    suspend fun flush() {
        flushUpdate()
        flushDelete()
        flushInsert()
    }

    fun delete(table: TableMeta<*>): Delete {
        return Delete(client, table, EmptyCondition)
    }

    private suspend fun flushUpdate() {
        tables.asSequence()
                .filter {
                    it.tableInstanceMeta.state == TableInstance.State.Fetch
                            && it.tableInstanceMeta.map != it.tableInstanceMeta.previousMap
                }
                .groupBy { it.tableInstanceMeta.tableMeta }
                .forEach { (tableMeta, tables) ->
                    val tuples = tables.map { table ->
                        Tuple.tuple().apply {
                            tableMeta.meta.columns.forEach { column ->
                                if (!column.primaryKey) {
                                    addValue(table.tableInstanceMeta.map[column.name])
                                }
                            }
                            addValue(table.tableInstanceMeta.map[tableMeta.meta.primaryKey.name])
                        }
                    }

                    val deferred = CompletableDeferred<AsyncResult<PgRowSet>>()
                    val sets = tableMeta
                            .meta
                            .columns
                            .asSequence()
                            .filter { !it.primaryKey }
                            .mapIndexed { index, column -> "${column.name} = \$${index + 1}" }
                            .joinToString()
                    val where = "${tableMeta.meta.primaryKey.name} = \$${tuples.first().size()}"
                    val updateQuery = "update ${tableMeta.meta.name} set $sets where ($where)"
                    client.preparedBatch(updateQuery, tuples) { ar -> deferred.complete(ar) }
                    val result = deferred.await()
                    if (result.failed()) {
                        throw result.cause()
                    }
                    tables.forEach { it.tableInstanceMeta.previousMap = it.tableInstanceMeta.map }
                }
    }

    private suspend fun flushDelete() {
        tables.asSequence()
                .filter { it.tableInstanceMeta.state == TableInstance.State.Delete }
                .groupBy { it.tableInstanceMeta.tableMeta }
                .forEach { (tableMeta, tables) ->
                    val primaryKeys = tables
                            .map { table -> table.tableInstanceMeta.map[tableMeta.meta.primaryKey.name] }
                            .let { tableMeta.meta.primaryKey.type.toArray(it) }
                            .let { Tuple.of(it) }
                    val deferred = CompletableDeferred<AsyncResult<PgRowSet>>()
                    val deleteQuery = "delete from ${tableMeta.meta.name} where ${tableMeta.meta.primaryKey.name} = ANY ($1)"
                    client.preparedQuery(deleteQuery, primaryKeys) { ar -> deferred.complete(ar) }
                    val result = deferred.await()
                    if (result.failed()) {
                        throw result.cause()
                    }
                    this.tables.removeAll(tables)
                }
    }

    private suspend fun flushInsert() {
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
                    tables.forEach {
                        it.tableInstanceMeta.state = TableInstance.State.Fetch
                        it.tableInstanceMeta.previousMap = it.tableInstanceMeta.map
                    }
                }
    }

    fun clear() {
        tables.clear()
    }

    internal fun <T : Result> select(values: List<SelectValue<*>>, mapper: (Row, List<RowGetter<*>>) -> T) =
            Select(this, values, null, null) { row, getters ->
                mapper(row, getters).apply {
                    this@Session.tables.addAll(this.getTables())
                }
            }

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