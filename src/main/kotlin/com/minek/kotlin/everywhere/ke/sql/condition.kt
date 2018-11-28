package com.minek.kotlin.everywhere.ke.sql

interface Condition : QueryPair

object EmptyCondition : Condition {
    override fun queryPair(index: Int): Pair<String, List<Pair<Type<Any?>, Any?>>>? {
        return null
    }
}

class AndCondition(private val left: Condition, private val right: Condition) : Condition {
    override fun queryPair(index: Int): Pair<String, List<Pair<StatementSetter<Any?>, Any?>>>? {
        val leftSql = left.queryPair(index)
        val rightSql = right.queryPair(index + (leftSql?.second?.size ?: 0))
        if (leftSql == null) {
            return rightSql
        }
        if (rightSql == null) {
            return null
        }
        return "(${leftSql.first}) and (${rightSql.first})" to (leftSql.second + rightSql.second)
    }
}

class OrCondition(private val left: Condition, private val right: Condition) : Condition {
    override fun queryPair(index: Int): Pair<String, List<Pair<StatementSetter<Any?>, Any?>>>? {
        val leftSql = left.queryPair(index)
        val rightSql = right.queryPair(index + (leftSql?.second?.size ?: 0))
        if (leftSql == null) {
            return rightSql
        }
        if (rightSql == null) {
            return null
        }
        return "(${leftSql.first}) or (${rightSql.first})" to (leftSql.second + rightSql.second)
    }
}

infix fun Condition.and(rh: Condition): Condition {
    return AndCondition(this, rh)
}

infix fun Condition.or(rh: Condition): Condition {
    return OrCondition(this, rh)
}
