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


    class Person : Table() {
        var pk by Person.pk
        var name by Person.name

        companion object : TableMeta<Person>() {
            val pk = column(IntType, primaryKey = true, autoIncrement = true)
            val name = column(StringType, default = "")
        }
    }

    @Test
    fun testCrud() = runBlocking {
        // delete previous data
        engine.session().run {
            select(Person)
                    .from(Person)
                    .all()
                    .map { it.component1() }
                    .forEach { delete(it) }
            flush()
        }

        // Insert
        engine.session().run {
            add(Person().apply { pk = 1; name = "john" })
            add(Person().apply { pk = 2; name = "jane" })
            flush()
        }

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

        session.delete(jane)
        session.flush()

        engine.session().run {
            val person = select(Person)
                    .from(Person)
                    .all()
                    .map { it.component1() }
                    .toList()

            assertEquals(1, person.size)
            assertEquals(1, person[0].pk)
            assertEquals("john", person[0].name)
        }
    }
}