package com.cpiz.leanmysqlimporter

/**
 * Created by caijw on 2017/2/18.
 *
 * 工具集
 */
fun safeDo(func: () -> Unit) {
    try {
        func()
    } catch (e: Exception) {
        println(e.message)
    }
}