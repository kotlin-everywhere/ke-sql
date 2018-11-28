package com.minek.kotlin.everywhere.ke.sql

interface OrderByValue {
    fun orderByQuery(index: Int): Pair<String, List<Pair<Type<Any?>, Any?>>>
}