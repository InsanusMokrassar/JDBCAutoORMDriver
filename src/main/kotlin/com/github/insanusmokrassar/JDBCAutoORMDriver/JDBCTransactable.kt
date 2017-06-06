package com.github.insanusmokrassar.JDBCAutoORMDriver

import com.github.insanusmokrassar.AutoORM.core.drivers.tables.interfaces.Transactable
import java.sql.Connection
import java.sql.Savepoint

class JDBCTransactable(private val connection: Connection) : Transactable {
    private var currentSavePoint : Savepoint? = null
    override fun start() {
        if (currentSavePoint == null) {
            connection.autoCommit = false
            currentSavePoint = connection.setSavepoint("${hashCode()}")
        } else {
            throw IllegalStateException("Transaction was started early")
        }
    }

    override fun abort() {
        if (currentSavePoint == null) {
            throw IllegalStateException("Transaction was not started")
        } else {
            connection.rollback(currentSavePoint)
            connection.releaseSavepoint(currentSavePoint)
            currentSavePoint = null
            connection.autoCommit = true
        }
    }

    override fun submit() {
        if (currentSavePoint == null) {
            throw IllegalStateException("Transaction was not started")
        } else {
            connection.commit()
            connection.releaseSavepoint(currentSavePoint)
            currentSavePoint = null
            connection.autoCommit = true
        }
    }
}