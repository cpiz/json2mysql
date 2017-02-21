package com.cpiz.json2mysql

import com.alibaba.fastjson.JSON
import com.alibaba.fastjson.JSONObject
import org.kohsuke.args4j.CmdLineException
import org.kohsuke.args4j.CmdLineParser
import org.kohsuke.args4j.Option
import org.kohsuke.args4j.spi.StringArrayOptionHandler
import java.io.BufferedReader
import java.io.File
import java.io.FileInputStream
import java.io.InputStreamReader
import java.util.*


/**
 * Created by caijw on 2017/2/18.
 *
 * 导入器逻辑
 */
class Importer {
    @Option(name = "-h", usage = "hostname")
    private var host: String = "localhost"

    @Option(name = "-P", usage = "port")
    private var port: Int = 3306

    @Option(name = "-u", usage = "username")
    private var username: String = "root"

    @Option(name = "-p", usage = "password")
    private var password: String = ""

    @Option(name = "-d", usage = "database", required = true)
    private var database: String = ""

    @Option(name = "-t", usage = "table", required = true)
    private var table: String = ""

    @Option(name = "-f", usage = "file", required = true)
    private var filePath: String = ""
    private var file: File? = null

    @Option(name = "-b", usage = "batch commit")
    private var batch: Int = 2000

    @Option(name = "-clear", usage = "clear table before import")
    private var clear: Boolean = false

    @Option(name = "-i", usage = "ignore column name", handler = StringArrayOptionHandler::class)
    private var ignores: List<String> = mutableListOf()

    lateinit private var parser: CmdLineParser

    @Suppress("DEPRECATION")
    fun exec(args: Array<String>) {
        parser = CmdLineParser(this)
        try {
            val x = ArrayList<String>()
            x.addAll(args)
            parser.parseArgument(x)

            when {
                database.isNullOrBlank() -> throw CmdLineException(parser, "Error: database is not given")
                table.isNullOrBlank() -> throw CmdLineException(parser, "Error: table is not given")
                filePath.isNullOrBlank() -> throw CmdLineException(parser, "Error: file is not given")
            }

            file = File(filePath)
            if (!file!!.exists()) {
                throw CmdLineException("file `$filePath` is not exist")
            }
            if (!file!!.isFile) {
                throw CmdLineException("path `$filePath` is not a file")
            }

//        println("host: $host")
//        println("port: $port")
//        println("username: $username")
//        println("password: $password")
//        println("database: $database")
//        println("filePath: $filePath")
//        println("table: $table")
//        println("ignores: $ignores")
        } catch (e: Exception) {
            System.err.println(e.message)
            System.err.println()
            printUsage()

            return
        }

        // 建立连接
        val db = Db()
        db.host = host
        db.port = port
        db.username = username
        db.password = password
        db.database = database
        db.ignores = ignores
        db.open()
//        dbModel.printSchemaInfo()

        // 读取文件
        val lines = getLineCount()
        println("data file line count: $lines")

        // 清空表
        if (clear) {
            try {
                db.truncateTable(table)
                println("table `$table` truncated")
            } catch (e: Exception) {
                System.err.println("truncate table `$table` error, ${e.message}")
            }
        }

        // 进行导入
        import(lines, db, table)
    }

    /**
     * 获得文件行数
     */
    fun getLineCount(): Int {
//        val lnr = LineNumberReader(FileReader(file))
//        lnr.use {
//            lnr.skip(java.lang.Long.MAX_VALUE)
//            val rows = lnr.lineNumber + 1
//            return rows
//        }

        var rows = 0
        readLines { num, line -> rows = num }
        return rows
    }

    /**
     * 处理每行数据
     */
    private fun readLines(func: (num: Int, str: String) -> Unit) {
        var isReader: InputStreamReader? = null
        var bufferedReader: BufferedReader? = null

        try {
            isReader = InputStreamReader(FileInputStream(file), "UTF8")// 考虑到编码格式
            bufferedReader = BufferedReader(isReader)

            var str: String?
            var num = 0
            do {
                str = bufferedReader.readLine()
                if (str != null) {
                    num++
                    func(num, str)
                } else {
                    break
                }
            } while (true)
        } catch (e: Exception) {
            System.err.println("Task interrupted")
        } finally {
            safeDo { bufferedReader?.close() }
            safeDo { isReader?.close() }
        }
    }

    /**
     * 导入数据
     */
    private fun import(totalNum: Int, db: Db, table: String) {
        val beginTime = System.currentTimeMillis()
        var insertNum = 0
        var newInsertNum = 0
        var percent = 0
        readLines { num, line ->
            if (insert(num, line, db, table)) {
                insertNum++
                newInsertNum++
            }

            if (num * 100 / totalNum > percent) {
                percent = num * 100 / totalNum
                printPercent(percent)
            }

            if (newInsertNum >= batch) {
                newInsertNum = 0
                db.commit()
            }
        }
        db.commit()
        println()
        println("$totalNum rows processed, $insertNum inserted" +
                ", ${"%.1f".format(((System.currentTimeMillis() - beginTime) / 1000f))}s")
        db.close()
    }

    /**
     * 插入一行
     */
    private fun insert(num: Int, str: String, db: Db, table: String): Boolean {
        val obj: JSONObject
        try {
            val newLine = str.trim().trim(',')
            if (!newLine.isNullOrEmpty()) {
                obj = JSON.parse(newLine) as JSONObject
            } else {
                System.err.println()
                System.err.println("[$num] skip empty line")
                return false
            }
        } catch (e: Exception) {
            System.err.println()
            System.err.println("[$num] parse `$str` error, ${e.message}")
            return false
        }

        try {
            db.insert(obj, table)
        } catch (e: Exception) {
            System.err.println()
            System.err.println("[$num] ${e.message}")
            return false
        }

        return true
    }

    private fun printUsage() {
        // print the list of available options
        System.err.println("json2mysql [options...] arguments...")
        System.err.println()
        System.err.println("example:")
        System.err.println("\tjson2mysql -d mydb -t mytable -clear -i col1 col2 -f D:\\MyDocs\\Desktop\\Sample.data")
        System.err.println()
        parser.printUsage(System.err)
        System.err.println()
    }

    /**
     * 输出百分比进度条
     * @param percent 百分比，0~100
     */
    private fun printPercent(percent: Int) {
        if (percent in 0..100) {
            System.out.print("\r|" +
                    (1..percent).joinToString(separator = "", transform = { ">" }) +
                    (percent..100 - 1).joinToString(separator = "", transform = { "-" }) +
                    "|$percent%")
        }
    }
}