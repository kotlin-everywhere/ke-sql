package com.minek.kotlin.everywhere.ke.sql

import io.reactiverse.pgclient.PgPool
import io.reactiverse.pgclient.PgRowSet
import io.reactiverse.pgclient.impl.ArrayTuple
import io.vertx.core.AsyncResult
import io.vertx.core.Handler
import kotlinx.coroutines.CompletableDeferred

data class Delete(private val pgPool: PgPool, private val from: TableMeta<*>, private val where: Condition) {
    fun where(condition: Condition): Delete {
        return copy(where = where and condition)
    }

    suspend fun execute() {
        val deleteFrom = "delete from ${from.meta.name}"
        val whereSql = where.queryPair(1)

        if (whereSql == null) {
            val deferred = PgDeferred()
            pgPool.query(deleteFrom, deferred)
            deferred.wait()
            return
        }

        val deferred = PgDeferred()
        pgPool.preparedQuery("$deleteFrom where ${whereSql.first}", ArrayTuple(whereSql.second), deferred)
        deferred.wait()
    }
}


class PgDeferred : Handler<AsyncResult<PgRowSet>> {
    private val deferred = CompletableDeferred<AsyncResult<PgRowSet>>()
    override fun handle(event: AsyncResult<PgRowSet>) {
        deferred.complete(event)
    }

    suspend fun wait(): PgRowSet {
        val result = deferred.await()
        if (result.failed()) {
            throw result.cause()
        }
        return result.result()
    }
}