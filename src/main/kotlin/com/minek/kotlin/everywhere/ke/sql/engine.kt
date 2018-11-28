package com.minek.kotlin.everywhere.ke.sql

import com.zaxxer.hikari.HikariConfig
import com.zaxxer.hikari.HikariDataSource

class Engine(database: String, user: String, password: String? = null, host: String? = null, maxSize: Int = 5, sendBufferSize: Int? = null) {
    private val dataSource =
            HikariConfig().apply {
                jdbcUrl = "jdbc:postgresql://localhost:5432/$database"
                username = user
                if (password != null) {
                    this.password = password
                }
                isAutoCommit = true
            }.let(::HikariDataSource)

    fun session(): Session {
        return Session(dataSource = dataSource)
    }

    fun close() {
        dataSource.close()
    }
}
