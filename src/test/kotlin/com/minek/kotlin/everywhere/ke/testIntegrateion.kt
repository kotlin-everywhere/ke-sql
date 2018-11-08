package com.minek.kotlin.everywhere.ke

import com.minek.kotlin.everywhere.ke.sql.*
import kotlinx.coroutines.runBlocking
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

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
        // remove previous data
        engine.session().run {
            select(Person)
                    .from(Person)
                    .all()
                    .map { it.component1() }
                    .forEach { remove(it) }
            flush()
        }

        // Insert
        engine.session().run {
            add(Person().apply { pk = 1; name = "john" })
            add(Person().apply { pk = 2; name = "jane" })
            flush()
        }

        engine.session().run {
            val people = select(Person)
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

        engine.session().run {
            val people = select(Person)
                    .from(Person)
                    .orderBy(Person.pk)
                    .all()
                    .map { it.component1() }
                    .toList()
            people[0].name = "was john"
            people[1].name = "was jane"
            flush()
        }

        engine.session().run {
            val people = select(Person)
                    .from(Person)
                    .orderBy(Person.pk)
                    .all()
                    .map { it.component1() }
                    .toList()

            assertEquals(2, people.size)

            val john = people[0]
            assertEquals(1, john.pk)
            assertEquals("was john", john.name)
            val jane = people[1]
            assertEquals(2, jane.pk)
            assertEquals("was jane", jane.name)
        }

        engine.session().run {
            val jane = select(Person)
                    .from(Person)
                    .orderBy(Person.pk)
                    .all()
                    .map { it.component1() }
                    .toList()[1]
            remove(jane)
            flush()
        }

        engine.session().run {
            val person = select(Person)
                    .from(Person)
                    .all()
                    .map { it.component1() }
                    .toList()

            assertEquals(1, person.size)
            assertEquals(1, person[0].pk)
            assertEquals("was john", person[0].name)
        }

        engine.session().run {
            // Insert
            engine.session().run {
                add(Person().apply { pk = 3; name = "hannibal" })
                flush()
            }

            delete(Person)
                    .where((Person.pk eq 1.v) or (Person.pk eq 3.v))
                    .execute()

            val people = select(Person)
                    .from(Person)
                    .all()
                    .map { it.component1() }
                    .toList()
            assertTrue(people.isEmpty())
        }
        Unit
    }
}