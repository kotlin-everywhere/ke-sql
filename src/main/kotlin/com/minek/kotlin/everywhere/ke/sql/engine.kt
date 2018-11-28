package com.minek.kotlin.everywhere.ke.sql

import io.reactiverse.pgclient.PgClient
import io.reactiverse.pgclient.PgPoolOptions

class Engine(database: String, user: String, password: String? = null, host: String? = null, maxSize: Int = 5, sendBufferSize: Int? = null) {
    private val client = PgPoolOptions()
            .run { if (host != null) setHost(host) else this }
            .setPort(5432)
            .setDatabase(database)
            .setUser(user)
            .run { if (password != null) setPassword(password) else this }
            .setMaxSize(maxSize)
            .run { if (sendBufferSize != null) setSendBufferSize(sendBufferSize) else this }
            .let(PgClient::pool)

    fun session(): Session {
        return Session(client = client)
    }

    fun close() = client.close()
}
