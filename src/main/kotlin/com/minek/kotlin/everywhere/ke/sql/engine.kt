package com.minek.kotlin.everywhere.ke.sql

import io.reactiverse.pgclient.PgClient
import io.reactiverse.pgclient.PgPoolOptions

class Engine(database: String, user: String, maxSize: Int = 5) {
    private val client = PgPoolOptions()
            .setPort(5432)
            .setDatabase(database)
            .setUser(user)
            .setMaxSize(maxSize)
            .let(PgClient::pool)

    fun session(): Session {
        return Session(client = client)
    }

    fun close() = client.close()
}
