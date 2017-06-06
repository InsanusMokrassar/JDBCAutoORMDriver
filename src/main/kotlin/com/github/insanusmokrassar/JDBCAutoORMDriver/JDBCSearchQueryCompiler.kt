package com.github.insanusmokrassar.JDBCAutoORMDriver

import com.github.insanusmokrassar.AutoORM.core.asSQLString
import com.github.insanusmokrassar.AutoORM.core.drivers.tables.SearchQuery
import com.github.insanusmokrassar.AutoORM.core.drivers.tables.filters.Filter


private val operations = mapOf(
        Pair(
                "eq",
                {
                    it: Filter ->
                    if (it.args[0] is String) {
                        it.args[0] = (it.args[0] as String).asSQLString()
                    }
                    if (it.isNot) {
                        "${it.field}!=${it.args[0]}"
                    } else {
                        "${it.field}=${it.args[0]}"
                    }
                }
        ),
        Pair(
                "is",
                {
                    it: Filter ->
                    if (it.args[0] is String) {
                        it.args[0] = (it.args[0] as String).asSQLString()
                    }
                    if (it.isNot) {
                        "${it.field}!=${it.args[0]}"
                    } else {
                        "${it.field}=${it.args[0]}"
                    }
                }
        ),
        Pair(
                "gt",
                {
                    it: Filter ->
                    if (it.isNot) {
                        "${it.field}<=${it.args[0]}"
                    } else {
                        "${it.field}>${it.args[0]}"
                    }
                }
        ),
        Pair(
                "gte",
                {
                    it: Filter ->
                    if (it.isNot) {
                        "${it.field}<${it.args[0]}"
                    } else {
                        "${it.field}>=${it.args[0]}"
                    }
                }
        ),
        Pair(
                "lt",
                {
                    it: Filter ->
                    if (it.isNot) {
                        "${it.field}>=${it.args[0]}"
                    } else {
                        "${it.field}<${it.args[0]}"
                    }
                }
        ),
        Pair(
                "lte",
                {
                    it: Filter ->
                    if (it.isNot) {
                        "${it.field}>${it.args[0]}"
                    } else {
                        "${it.field}<=${it.args[0]}"
                    }
                }
        ),
        Pair(
                "in",
                {
                    it: Filter ->
                    if (it.isNot) {
                        "${it.field}<${it.args[0]} OR ${it.field}>${it.args[1]}"
                    } else {
                        "${it.field}>=${it.args[0]} AND ${it.field}<=${it.args[1]}"
                    }
                }
        ),
        Pair(
                "oneof",
                {
                    filter: Filter ->
                    val localBuilder = StringBuilder()
                    val operator: String
                    if (filter.isNot) {
                        operator = "!="
                    } else {
                        operator = "="
                    }
                    localBuilder.append("(")
                    filter.args.forEach {
                        localBuilder.append("${filter.field}$operator$it")
                        if (filter.args.indexOf(it) < filter.args.size - 1) {
                            localBuilder.append(" OR ")
                        }
                    }
                    localBuilder.append(")")
                }
        )
)

object JDBCSearchQueryCompiler {
    fun compilePaging(query: SearchQuery): String {
        if (query.pageFilter != null) {
            val offset = query.pageFilter!!.page * query.pageFilter!!.size
            return " LIMIT ${query.pageFilter!!.size} OFFSET $offset"
        } else {
            return ""
        }
    }

    fun compileQuery(query: SearchQuery): String {
        if (query.filters.isNotEmpty()) {
            val queryBuilder = StringBuilder().append(" WHERE ")

            query.filters.forEach {
                if (operations.contains(it.filterName)) {
                    queryBuilder.append(
                            operations[it.filterName]!!(it)
                    )
                    if (it.logicalLink != null) {
                        queryBuilder.append(
                                " ${it.logicalLink} "
                        )
                    }
                } else {
                    throw IllegalStateException("Unsupported filter \"${it.filterName}\"")
                }
            }

            return queryBuilder.toString()
        } else {
            return ""
        }
    }
}