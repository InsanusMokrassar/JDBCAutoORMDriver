package com.github.insanusmokrassar.JDBCAutoORMDriver

import com.github.insanusmokrassar.AutoORM.core.*
import com.github.insanusmokrassar.AutoORM.core.compilers.OperationsCompiler
import com.github.insanusmokrassar.AutoORM.core.drivers.tables.abstracts.AbstractTableProvider
import com.github.insanusmokrassar.AutoORM.core.drivers.tables.SearchQuery
import java.sql.Connection
import kotlin.reflect.KCallable
import kotlin.reflect.KClass
import kotlin.reflect.KProperty

class JDBCTableProvider<M : Any, O : M> (
        modelClass: KClass<M>,
        operationsClass: KClass<in O>,
        operationsCompiler: OperationsCompiler,
        val connection: Connection)
    : AbstractTableProvider<M, O>(
        operationsCompiler,
        modelClass,
        operationsClass) {

    protected val primaryFields: List<KCallable<*>> = modelClass.getPrimaryFields()

    override fun remove(where: SearchQuery): Boolean {
        val queryBuilder = StringBuilder().append("DELETE FROM ${modelClass.simpleName} ${JDBCSearchQueryCompiler.compileQuery(where)}${JDBCSearchQueryCompiler.compilePaging(where)};")
        val statement = connection.prepareStatement(queryBuilder.toString())
        return statement.execute()
    }

    override fun find(where: SearchQuery): Collection<O> {
        checkSearchCompileQuery(where)
        val queryBuilder = StringBuilder().append("SELECT ")
        if (where.fields.isEmpty()) {
            queryBuilder.append("* ")
        } else {
            where.fields.forEach {
                queryBuilder.append(it)
                if (!where.fields.isLast(it)) {
                    queryBuilder.append(",")
                }
            }
        }
        queryBuilder.append(" FROM ${modelClass.simpleName} ${JDBCSearchQueryCompiler.compileQuery(where)}${JDBCSearchQueryCompiler.compilePaging(where)};")

        val resultSet = connection.prepareStatement(queryBuilder.toString()).executeQuery()
        val result = ArrayList<O>()
        while (resultSet.next()) {
            val currentValuesMap = HashMap<KProperty<*>, Any>()
            if (where.fields.isEmpty()) {
                variablesMap.values.forEach {
                    currentValuesMap.put(it, resultSet.getObject(it.name, it.returnClass().java))
                }
            } else {
                where.fields.forEach {
                    val currentProperty = variablesMap[it]!!
                    currentValuesMap.put(currentProperty, resultSet.getObject(it, currentProperty.returnClass().javaObjectType))
                }
            }
            result.add(createModelFromValuesMap(currentValuesMap))
        }
        return result
    }

    override fun insert(values: Map<KProperty<*>, Any>): Boolean {
        val queryBuilder = StringBuilder().append("INSERT INTO ${modelClass.simpleName}")
        val fieldsBuilder = StringBuilder()
        val valuesBuilder = StringBuilder()
        val valuesList = values.toList()
        valuesList.forEach {
            fieldsBuilder.append(it.first.name)
            if (it.second is String) {
                valuesBuilder.append((it.second as String).asSQLString())
            } else {
                valuesBuilder.append(it.second.toString())
            }
            if (valuesList.indexOf(it) < valuesList.size - 1) {
                fieldsBuilder.append(",")
                valuesBuilder.append(",")
            }
        }
        queryBuilder.append(" ($fieldsBuilder) VALUES ($valuesBuilder);")
        val statement = connection.prepareStatement(queryBuilder.toString())
        return statement.execute()
    }

    override fun update(values: Map<KProperty<*>, Any>, where: SearchQuery): Boolean {
        val queryBuilder = StringBuilder().append("UPDATE ${modelClass.simpleName} SET ")
        values.forEach {
            if (it.value is String) {
                queryBuilder.append(" ${it.key.name}=\'${it.value}\'")
            } else {
                queryBuilder.append(" ${it.key.name}=${it.value}")
            }
            if (!values.keys.isLast(it.key)) {
                queryBuilder.append(",")
            }
        }
        queryBuilder.append("${JDBCSearchQueryCompiler.compileQuery(where)}${JDBCSearchQueryCompiler.compilePaging(where)};")
        val statement = connection.prepareStatement(queryBuilder.toString())
        return statement.execute()
    }

    protected fun checkSearchCompileQuery(query : SearchQuery) {
        if (primaryFields.isNotEmpty() && !query.fields.containsAll(primaryFields.select({it.name}))) {
            query.fields.addAll(primaryFields.select({it.name}))
        }
    }
}
