package com.minek.kotlin.everywhere.ke

import com.minek.kotlin.everywhere.ke.sql.Engine
import com.minek.kotlin.everywhere.ke.sql.plus
import com.minek.kotlin.everywhere.ke.sql.select
import com.minek.kotlin.everywhere.ke.sql.v
import kotlinx.coroutines.experimental.runBlocking
import kotlin.test.Test
import kotlin.test.assertEquals

class TestIntegration {
    @Test
    fun testConnection() = runBlocking {
        val engine = Engine(database = "ke-sql-test", user = System.getProperty("user.name"))
        val session = engine.session()
        val (answer) = session
                .select(1.v plus 2.v)
                .one()
        assertEquals(3, answer)
    }
}