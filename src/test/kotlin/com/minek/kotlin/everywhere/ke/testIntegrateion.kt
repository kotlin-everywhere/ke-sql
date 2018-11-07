package com.minek.kotlin.everywhere.ke

import com.minek.kotlin.everywhere.ke.sql.*
import kotlinx.coroutines.runBlocking
import kotlin.test.Test
import kotlin.test.assertEquals

class TestIntegration {
    private val engine = Engine(database = "ke-sql-test", user = System.getProperty("user.name"))

    @Test
    fun testOnePlus2() = runBlocking {
        val session = engine.session()
        //  1 + 2 == 3
        val (three) = session
                .select(1.v plus 2.v)
                .one()
        assertEquals(3, three)
    }


    private class Person : Table() {
        val pk by Person.pk
        var name by Person.name

        companion object : TableMeta<Person>() {
            val pk = column(IntType, primaryKey = true, autoIncrement = true)
            val name = column(StringType, default = "")
        }
    }

    @Test
    fun testTableSelect() = runBlocking {
        val session = engine.session()
        // 기본 Table Select 테스트
        val people = session
                .select(Person)
                .from(Person)
                .orderBy(Person.pk)
                .all()
                .map { it.component1() }
                .toList()
        assertEquals(2, people.size)

        val john = people[0]
        assertEquals(1, john.pk)
        assertEquals("john", john.name)
        val jane = people[1]
        assertEquals(2, jane.pk)
        assertEquals("jane", jane.name)
    }

    class Job : Table() {
        var pk by Job.pk
        var name by Job.name

        companion object : TableMeta<Job>() {
            val pk = column(IntType, primaryKey = true, autoIncrement = true)
            val name = column(StringType, default = "")
        }
    }

    @Test
    fun testInsert() = runBlocking {
        engine.session().run {
            add(Job().apply { pk = 1; name = "Pianist" })
            add(Job().apply { pk = 2; name = "Cook" })
            flush()
        }

        val jobs = engine.session()
                .select(Job)
                .from(Job)
                .all()
                .map { it.component1() }
                .toList()

        assertEquals(2, jobs.size)
        assertEquals(1, jobs[0].pk)
        assertEquals("Pianist", jobs[0].name)
        assertEquals(2, jobs[1].pk)
        assertEquals("Cook", jobs[1].name)
    }
}