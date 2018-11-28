package com.minek.kotlin.everywhere.ke.sql

import kotlinx.coroutines.ExecutorCoroutineDispatcher
import kotlinx.coroutines.withContext
import java.sql.Connection
import java.sql.ResultSet
import javax.sql.DataSource


class Session(private val ctx: ExecutorCoroutineDispatcher, private val dataSource: DataSource) {
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
        return Delete(this, table, EmptyCondition)
    }

    internal suspend inline fun <T> io(crossinline block: (connection: Connection) -> T): T {
        return withContext(ctx) {
            dataSource.connection.use(block)
        }
    }

    private suspend fun flushUpdate() {
        tables.asSequence()
                .filter {
                    it.tableInstanceMeta.state == TableInstance.State.Fetch
                            && it.tableInstanceMeta.map != it.tableInstanceMeta.previousMap
                }
                .groupBy { it.tableInstanceMeta.tableMeta }
                .forEach { (tableMeta, tables) ->
                    val valueColumns = tableMeta.meta.columns.filter { !it.primaryKey }
                    val sets =
                            valueColumns
                                    .mapIndexed { index, column -> "${column.name} = ?" }
                                    .joinToString()
                    val where = "${tableMeta.meta.primaryKey.name} = ?"
                    val updateQuery = "update ${tableMeta.meta.name} set $sets where ($where)"
                    io { connection ->
                        connection.prepareStatement(updateQuery).use { statement ->
                            tables.forEach { table ->
                                valueColumns.forEachIndexed { index, column ->
                                    if (!column.primaryKey) {
                                        (column.type as Type<Any?>).set(statement, index + 1, table.tableInstanceMeta.map[column.name])
                                    }
                                }
                                (tableMeta.meta.primaryKey.type as Type<Any?>).set(statement, valueColumns.size + 1, table.tableInstanceMeta.map[tableMeta.meta.primaryKey.name])
                                statement.addBatch()
                            }
                            statement.executeBatch()
                        }
                    }

                    tables.forEach { it.tableInstanceMeta.previousMap = it.tableInstanceMeta.map }
                }
    }

    private suspend fun flushDelete() {
        tables.asSequence()
                .filter { it.tableInstanceMeta.state == TableInstance.State.Delete }
                .groupBy { it.tableInstanceMeta.tableMeta }
                .forEach { (tableMeta, tables) ->
                    val primaryKeyValues = Array(tables.size) { index ->
                        tables[index].tableInstanceMeta.map[tableMeta.meta.primaryKey.name]
                    }
                    io { connection ->
                        val primaryKeys = (tableMeta.meta.primaryKey.type as Type<Any?>).arrayOf(connection, primaryKeyValues)
                        val deleteQuery = "delete from ${tableMeta.meta.name} where ${tableMeta.meta.primaryKey.name} = ANY (?)"
                        connection.prepareStatement(deleteQuery).use { statement ->
                            statement.setArray(1, primaryKeys)
                            statement.execute()
                        }
                    }
                    this.tables.removeAll(tables)
                }
    }

    private suspend fun flushInsert() {
        tables.asSequence()
                .filter { it.tableInstanceMeta.state == TableInstance.State.New }
                .groupBy { it.tableInstanceMeta.tableMeta }
                .forEach { (tableMeta, tables) ->
                    val columns = tableMeta.meta.columns.joinToString() { it.name }
                    val values = (1..tableMeta.meta.columns.size).joinToString(",") { "?" }
                    val insertQuery = "insert into ${tableMeta.meta.name} ($columns) values ($values)"
                    io { connection ->
                        connection.prepareStatement(insertQuery).use { statement ->
                            tables.forEach { table ->
                                tableMeta.meta.columns.forEachIndexed { index, column ->
                                    (column.type as Type<Any?>).set(statement, index + 1, table.tableInstanceMeta.map[column.name])
                                }
                                statement.addBatch()
                            }
                            statement.executeBatch()
                        }
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

    internal fun <T : Result> select(values: List<SelectValue<*>>, mapper: (ResultSet, List<RowGetter<*>>) -> T) =
            Select(this, values, null, null) { row, getters ->
                mapper(row, getters).apply {
                    this@Session.tables.addAll(this.getTables())
                }
            }

    internal suspend fun <T> preparedQuery(sql: String, arguments: List<Pair<StatementSetter<Any?>, Any?>>, resultMapper: (ResultSet) -> T): T {
        return io { connection ->
            connection.prepareStatement(sql).use { preparedStatement ->
                arguments.forEachIndexed { index, (type, value) ->
                    type.set(preparedStatement, index + 1, value)
                }
                preparedStatement.executeQuery().use(resultMapper)
            }
        }
    }
}

fun <T1> Session.select(value: SelectValue<T1>): Select<Result1<T1>> {
    return select(listOf(value), ::Result1)
}