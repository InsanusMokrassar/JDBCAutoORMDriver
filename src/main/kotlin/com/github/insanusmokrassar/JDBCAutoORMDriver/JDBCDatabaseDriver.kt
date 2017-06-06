package com.github.insanusmokrassar.JDBCAutoORMDriver

import com.github.insanusmokrassar.AutoORM.core.DatabaseConnect
import com.github.insanusmokrassar.AutoORM.core.drivers.databases.interfaces.DatabaseDriver
import com.github.insanusmokrassar.iobjectk.interfaces.IObject
import java.sql.DriverManager
import kotlin.reflect.KClass

class JDBCDatabaseDriver(parameters: IObject<Any>) : DatabaseDriver {

    init {
        val driver = parameters.get<String>("jdbcDriverPath")
        Class.forName(driver)
    }
    override fun getDatabaseConnect(params: IObject<Any>, onFreeCallback: (DatabaseConnect) -> Unit, onCloseCallback: (DatabaseConnect) -> Unit): DatabaseConnect {
        val connection = DriverManager.getConnection(
                params.get("url"),
                params.get("username"),
                params.get("password")
        )
        return DatabaseConnect(
                JDBCTableDriver(connection),
                JDBCTransactable(connection),
                onFreeCallback,
                onCloseCallback
        )
    }

    override fun supportTable(modelClass: KClass<*>): Boolean {
        return true
    }
}