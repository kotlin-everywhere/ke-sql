package com.minek.kotlin.everywhere.ke.sql

data class Delete(private val session: Session, private val from: TableMeta<*>, private val where: Condition) {
    fun where(condition: Condition): Delete {
        return copy(where = where and condition)
    }

    suspend fun execute() {
        val deleteFrom = "delete from ${from.meta.name}"
        val whereSql = where.queryPair(1)

        if (whereSql == null) {
            session.io { connection ->
                connection.createStatement().use { statement ->
                    statement.execute(deleteFrom)
                }
            }

            return
        }

        session.io { connection ->
            connection.prepareStatement("$deleteFrom where ${whereSql.first}").use { preparedStatement ->
                whereSql.second.forEachIndexed { index, (type, value) ->
                    type.set(preparedStatement, index + 1, value)
                }
                preparedStatement.execute()
            }
        }
    }
}

