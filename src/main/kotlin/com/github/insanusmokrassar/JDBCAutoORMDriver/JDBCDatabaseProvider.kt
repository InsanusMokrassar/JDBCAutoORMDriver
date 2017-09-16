package com.github.insanusmokrassar.JDBCAutoORMDriver

import com.github.insanusmokrassar.AutoORM.core.drivers.databases.abstracts.AbstractDatabaseProvider
import com.github.insanusmokrassar.AutoORM.core.drivers.tables.interfaces.ConnectionProvider
import com.github.insanusmokrassar.AutoORM.core.drivers.tables.interfaces.Transactable
import com.github.insanusmokrassar.IObjectK.interfaces.IObject
import java.sql.DriverManager
import kotlin.reflect.KClass

class JDBCDatabaseProvider(params: IObject<Any>) : AbstractDatabaseProvider() {
    init {
        val driver = params.get<String>("jdbcDriverPath")
        Class.forName(driver)
    }

    override fun makeDriverAndTransactable(params: IObject<Any>): Pair<ConnectionProvider, Transactable> {
        val connection = DriverManager.getConnection(
                params.get("url"),
                params.get("username"),
                params.get("password")
        )
        return Pair(
                JDBCConnectionProvider(connection),
                JDBCTransactable(connection)
        )
    }

    override fun supportTable(modelClass: KClass<*>): Boolean {
        return true
    }
}