package com.cpiz.json2mysql

import com.alibaba.fastjson.JSONObject
import java.sql.*

/**
 * Created by caijw on 2017/2/18.
 *
 * 数据库Model，用于数据库操作
 */
class DbModel {
    var host: String = "localhost"
    var port: Int = 3306
    var username: String = "root"
    var password: String = ""
    var database: String = ""
    var table: String = ""
    var conn: Connection? = null
    var ignores: List<String> = mutableListOf()

    /**
     * 打开数据库连接
     */
    fun open(): Boolean {
        try {
            conn = DriverManager.getConnection(
                    "jdbc:mysql://$host:$port/$database" +
                            "?useUnicode=true" +
                            "&useJDBCCompliantTimezoneShift=true" +
                            "&useLegacyDatetimeCode=false" +
                            "&serverTimezone=UTC" +
                            "&characterEncoding=utf8" +
                            "&connectionCollation=utf8_general_ci" +
                            "&characterSetResults=utf8",
                    username, password)
            conn?.autoCommit = false
        } catch (e: SQLException) {
            println(e.message)
        }

        return if (conn != null) {
            !conn!!.isClosed
        } else {
            false
        }
    }

    /**
     * 关闭数据库
     */
    fun close() {
        safeDo { conn?.close() }
    }

    /**
     * 插入数据
     */
    fun insert(obj: JSONObject): Boolean {
        if (conn == null || conn!!.isClosed) {
            throw SQLException("Connect is closed")
        }

        val keys = obj.keys.filterNot { ignores.any { s -> s.toUpperCase() == it.toUpperCase() } }
        if (keys.isEmpty()) {
            System.err.println("No column to insert!")
            return false
        }

        val sql = "insert into `$database`.`$table` (${keys.joinToString()}) values (${keys.joinToString(transform = { s -> "?" })})"
        val stmt = conn!!.prepareStatement(sql)
        for (i in 0..keys.size - 1) {
            stmt.setString(i + 1, obj.getString(keys[i]))
        }

        try {
            stmt.execute()
            return true
        } catch (e: SQLException) {
            System.err.println(sql)
            for (i in 0..keys.size - 1) {
                if (i != 0) {
                    System.err.print(", ")
                }
                System.err.print("${keys[i]}: \"${obj.getString(keys[i])}\"")
                if (i == keys.size - 1) {
                    System.err.print('\n')
                }
            }
            System.err.println(e.message)
            System.err.println()

            return false
        } finally {
            safeDo { stmt?.close() }
        }
    }

    /**
     * 提交事务
     */
    fun commit() {
        conn?.commit()
    }

    fun printSchemaInfo() {
        var stmt: Statement? = null
        var rs: ResultSet? = null
        try {
            stmt = conn!!.createStatement()

            val sql = "SELECT COLUMN_NAME, ORDINAL_POSITION, COLUMN_DEFAULT, IS_NULLABLE, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH" +
                    " FROM INFORMATION_SCHEMA.columns" +
                    " WHERE table_schema='$database'" +
                    " AND table_name='$table'" +
                    " ORDER BY ORDINAL_POSITION"
            rs = stmt.executeQuery(sql)
            while (rs.next()) {
                print("COLUMN_NAME: `${rs.getString("COLUMN_NAME")}`" +
                        ", ORDINAL_POSITION: `${rs.getInt("ORDINAL_POSITION")}`" +
                        ", COLUMN_DEFAULT: `${rs.getString("COLUMN_DEFAULT")}`" +
                        ", IS_NULLABLE: `${rs.getString("IS_NULLABLE")}`" +
                        ", DATA_TYPE: `${rs.getString("DATA_TYPE")}`" +
                        ", CHARACTER_MAXIMUM_LENGTH: `${rs.getInt("CHARACTER_MAXIMUM_LENGTH")}`\n")
            }
        } catch (e: SQLException) {
            System.err.println(e.message)
        } finally {
            safeDo { rs?.close() }
            safeDo { stmt?.close() }
        }
    }
}