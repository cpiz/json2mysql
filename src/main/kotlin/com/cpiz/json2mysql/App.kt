package com.cpiz.json2mysql

/**
 * Created by caijw on 2017/2/20.
 *
 * APP入口
 */
class App {
    companion object {
        @JvmStatic fun main(args: Array<String>) {
            val a = Importer()
            a.exec(args)
        }
    }
}
