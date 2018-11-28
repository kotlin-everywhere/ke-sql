package com.minek.kotlin.everywhere.ke.sql

import com.zaxxer.hikari.HikariConfig
import com.zaxxer.hikari.HikariDataSource
import kotlinx.coroutines.asCoroutineDispatcher
import kotlinx.coroutines.newFixedThreadPoolContext
import java.util.concurrent.Executors
import java.util.concurrent.ThreadFactory
import java.util.concurrent.atomic.AtomicInteger

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

    private val ctx = Executors.newFixedThreadPool(dataSource.maximumPoolSize, DefaultThreadFactory()).asCoroutineDispatcher()

    fun session(): Session {
        dataSource.maximumPoolSize
        return Session(ctx = ctx, dataSource = dataSource)
    }

    fun close() {
        dataSource.close()
    }

    private class DefaultThreadFactory : ThreadFactory {
        private val group: ThreadGroup
        private val threadNumber = AtomicInteger(1)
        private val namePrefix: String

        init {
            val s = System.getSecurityManager()
            group = if (s != null)
                s.threadGroup
            else
                Thread.currentThread().threadGroup
            namePrefix = "ke-sql-database-io-pool-" +
                    poolNumber.getAndIncrement() +
                    "-thread-"
        }

        override fun newThread(r: Runnable): Thread {
            val t = Thread(group, r,
                    namePrefix + threadNumber.getAndIncrement(),
                    0)
            if (t.isDaemon)
                t.isDaemon = false
            if (t.priority != Thread.NORM_PRIORITY)
                t.priority = Thread.NORM_PRIORITY
            return t
        }

        companion object {
            private val poolNumber = AtomicInteger(1)
        }
    }
}
