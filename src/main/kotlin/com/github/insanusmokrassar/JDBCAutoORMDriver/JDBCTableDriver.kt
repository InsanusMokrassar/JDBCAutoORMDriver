package com.github.insanusmokrassar.JDBCAutoORMDriver

import com.github.insanusmokrassar.AutoORM.core.*
import com.github.insanusmokrassar.AutoORM.core.drivers.tables.interfaces.TableDriver
import com.github.insanusmokrassar.AutoORM.core.drivers.tables.interfaces.TableProvider
import java.sql.Connection
import java.util.logging.Logger
import kotlin.reflect.KClass

class JDBCTableDriver(private val connection: Connection) : TableDriver {
    override fun <M : Any, O : M> getTableProvider(modelClass: KClass<M>, operationsClass: KClass<in O>): TableProvider<M, O> {
        createTableIfNotExist(modelClass)
        return JDBCTableProvider(modelClass, operationsClass, connection)
    }
    override fun close() {
        connection.close()
    }

    protected fun <M : Any, O : M> createTableIfNotExist(modelClass: KClass<M>) {
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
}