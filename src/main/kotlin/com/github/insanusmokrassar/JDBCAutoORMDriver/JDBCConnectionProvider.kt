package com.github.insanusmokrassar.JDBCAutoORMDriver

import com.github.insanusmokrassar.AutoORM.core.*
import com.github.insanusmokrassar.AutoORM.core.drivers.tables.interfaces.ConnectionProvider
import com.github.insanusmokrassar.AutoORM.core.drivers.tables.interfaces.TableProvider
import com.github.insanusmokrassar.AutoORM.core.generators.RealisationsGenerator
import java.sql.Connection
import java.util.logging.Logger
import kotlin.reflect.KClass

val nativeTypesMap = mapOf(
        Pair(
                Int::class,
                "INTEGER"
        ),
        Pair(
                Long::class,
                "LONG"
        ),
        Pair(
                Float::class,
                "FLOAT"
        ),
        Pair(
                Double::class,
                "DOUBLE"
        ),
        Pair(
                String::class,
                "TEXT"
        ),
        Pair(
                Boolean::class,
                "BOOLEAN"
        )
)

class JDBCConnectionProvider(private val connection: Connection) : ConnectionProvider {
    override fun <M : Any, O : M> getTableProvider(
            generator: RealisationsGenerator,
            modelClass: KClass<M>,
            operationsClass: KClass<in O>): TableProvider<M, O> {
        createTableIfNotExist(modelClass)
        return JDBCTableProvider(modelClass, operationsClass, generator, connection)
    }

    protected fun <M : Any> createTableIfNotExist(modelClass: KClass<M>) {
        val fieldsBuilder = StringBuilder()
        val primaryFields = modelClass.getPrimaryFields()

        modelClass.getVariables().forEach {
            if (it.isReturnNative()) {
                fieldsBuilder.append("${it.name} ${nativeTypesMap[it.returnClass()]}")
                if (!it.isNullable()) {
                    fieldsBuilder.append(" NOT NULL")
                }
                if (primaryFields.contains(it) && it.isAutoincrement()) {
                    fieldsBuilder.append(" AUTO_INCREMENT")
                }
            } else {
                TODO()
            }
            fieldsBuilder.append(", ")
        }
        if (primaryFields.isNotEmpty()) {
            fieldsBuilder.append("CONSTRAINT ${modelClass.simpleName}_PR_KEY PRIMARY KEY (")
            primaryFields.forEach {
                fieldsBuilder.append(it.name)
                if (!primaryFields.isLast(it)) {
                    fieldsBuilder.append(", ")
                }
            }
            fieldsBuilder.append(")")
        }

        try {
            if (connection.prepareStatement("CREATE TABLE IF NOT EXISTS ${modelClass.simpleName} ($fieldsBuilder);").execute()) {
                Logger.getGlobal().info("Table ${modelClass.simpleName} was created")
            }
        } catch (e: Exception) {
            Logger.getGlobal().throwing(this::class.simpleName, "init", e)
            throw IllegalArgumentException("Can't create table ${modelClass.simpleName}", e)
        }
    }

    override fun close() {
        connection.close()
    }
}