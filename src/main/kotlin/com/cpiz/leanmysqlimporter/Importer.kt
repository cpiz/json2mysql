package com.cpiz.leanmysqlimporter

import org.kohsuke.args4j.CmdLineException
import org.kohsuke.args4j.CmdLineParser
import org.kohsuke.args4j.Option
import org.kohsuke.args4j.spi.StringArrayOptionHandler
import java.util.*

/**
 * Created by caijw on 2017/2/18.
 *
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

    @Option(name = "-d", usage = "database")
    private var database: String = ""

    @Option(name = "-t", usage = "table")
    private var table: String = ""

    @Option(name = "-f", usage = "file")
    private var filePath: String = ""

    @Option(name = "-b", usage = "batch commit")
    private var batch: Int = 2000

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
                database.isNullOrBlank() -> throw CmdLineException(parser, "database is not given")
                table.isNullOrBlank() -> throw CmdLineException(parser, "table is not given")
                filePath.isNullOrBlank() -> throw CmdLineException(parser, "file is not given")
            }

            // 建立数据库连接
            val model = DbModel()
            model.host = host
            model.port = port
            model.username = username
            model.password = password
            model.database = database
            model.table = table
            model.ignores = ignores
            model.open()

//            model.printSchemaInfo()

            // 解析文件
            val dataModel = DataModel(filePath)
            val lines = dataModel.getLineCount()
            println("file line count: $lines")

            // 插入数据
            val beginTime = System.currentTimeMillis()
            for (i in 1..100) {
                print("_")
                if (i == 100) {
                    print("\n")
                }
            }

            var currentNum = 0
            var insertNum = 0
            var percent = 1
            dataModel.parseJsonObjects({ obj ->
                if (model.insert(obj)) {
                    insertNum++
                }
                currentNum++

                if (currentNum * 100 / lines >= percent) {
                    percent++
                    print(">")
                }

                if (insertNum % batch == 0) {
                    model.commit()
                }
            })
            model.commit()
            print("\n")
            println("$currentNum rows processed, $insertNum inserted, cost ${System.currentTimeMillis() - beginTime}ms")
            model.close()

        } catch (e: Exception) {
            // if there's a problem in the command line,
            // you'll get this exception. this will report
            // an error message.
            System.err.println(e.message)
            printUsage()

            return
        }

//        println("host: $host")
//        println("port: $port")
//        println("username: $username")
//        println("password: $password")
//        println("database: $database")
//        println("filePath: $filePath")
//        println("table: $table")
//        println("ignores: $ignores")
    }

    fun printUsage() {
        // print the list of available options
        System.err.println("LeanMysqlImporter [options...] arguments...")
        parser.printUsage(System.err)
        System.err.println()
    }
}