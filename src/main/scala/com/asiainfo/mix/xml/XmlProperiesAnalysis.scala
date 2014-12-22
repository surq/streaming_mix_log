package com.asiainfo.mix.xml

import scala.xml.XML
import scala.collection.mutable.Map
import scala.collection.mutable.ArrayBuffer
import scala.beans.BeanProperty
/**
 * @author surq
 * @since 2014.09.19
 * ［配置文件XML解析］功能解介：<br>
 * 把conf/logbatchconf.xml解析返回解析结果值<br>
 */
object XmlProperiesAnalysis {

  @BeanProperty val commonPropsMap = Map[String, String]()
  @BeanProperty val dataInputMap = Map[String, Map[String, String]]()
  @BeanProperty val dbSourceMap = Map[String, String]()
  @BeanProperty val logStructMap = Map[String, Map[String, String]]()
  @BeanProperty val tablesDefMap = Map[String, String]()

  val ox002: Char = 2
  val defSeparator = ox002.toString
  
  def main(arge: Array[String]) {
    XmlProperiesAnalysis.getXmlProperies

    val value = XmlProperiesAnalysis.getCommonPropsMap
    println("------------properiesMap:applacation---------------")
    value foreach println

    val path = XmlProperiesAnalysis.getDataInputMap
    println("-------------HDFSfilePathMap--------------")
    path.map(f => {
      println(f._1)
      f._2 foreach println
    })

    println("-------------dbSourceMap:db--------------")
    val db = XmlProperiesAnalysis.getDbSourceMap
    db foreach println
    println("-------------logStructMap：log--------------")
    val log = XmlProperiesAnalysis.getLogStructMap
    log.map(f => {
      println(f._1)
      f._2 foreach println
    })
    val tab = XmlProperiesAnalysis.getTablesDefMap
    println("-------------tablesDefMap：mysql--------------")
    tab foreach println
  }

  def getXmlProperies = {

    val xmlFile = XML.load("conf/logconf.xml")

    //-----------------applacation  commonProps 配置-------------------------
    val properies = xmlFile \ "commonProps"
    val appName = (properies \ "appName").text.toString.trim
    val interval = (properies \ "interval").text.toString.trim
    val logSteps = (properies \ "logSteps").text.toString.trim
    commonPropsMap += ("appName" -> appName)
    commonPropsMap += ("interval" -> interval)
    commonPropsMap += ("logSteps" -> logSteps)

    //-----------------输入日志文件类型以及路径(HDFS)配置 dataInput----------------    
    val input = xmlFile \ "dataInput" \ "input"
    input.foreach(f => {
      val logType = (f \ "logType").text.toString.trim
      val appClass = (f \ "appClass").text.toString.trim
      val dir = (f \ "dir").text.toString.trim

      val inputMap = Map[String, String]()
      inputMap += ("logType" -> logType)
      inputMap += ("appClass" -> appClass)
      inputMap += ("dir" -> dir)

      dataInputMap += (logType -> inputMap)
    })

    //-----------------db(mysql) 驱动配置-----------------------------------
    val dbconf = xmlFile \ "dataoutput" \ "dbSource"
    val driver = (dbconf \ "driver").text.toString.trim
    val url = (dbconf \ "url").text.toString.trim
    val user = (dbconf \ "user").text.toString
    val password = (dbconf \ "password").text.toString.trim

    dbSourceMap += ("driver" -> driver)
    dbSourceMap += ("url" -> url)
    dbSourceMap += ("user" -> user)
    dbSourceMap += ("password" -> password)

    //-----------------log 日志属性配置--------------------------------------

    val mixlogs = xmlFile \ "logProperties" \ "log"
    mixlogs.map(p => {
      val mixLogMap = Map[String, String]()
      val logType = (p \ "logType").text.toString.trim()
      val items = (p \ "items").text.toString.trim()
      val itemsDescribe = (p \ "itemsDescribe").text.toString.trim()
      val rowKey = (p \ "rowKey").text.toString.trim()
      var separator = (p \ "separator").text.toString
      if (separator == "") separator = defSeparator
      mixLogMap += ("logType" -> logType)
      mixLogMap += ("items" -> items)
      mixLogMap += ("itemsDescribe" -> itemsDescribe)
      mixLogMap += ("rowKey" -> rowKey)
      mixLogMap += ("separator" -> separator)

      logStructMap += (logType -> mixLogMap)
    })
    //-----------------mysql表定义配置---------------------------------------    
    val tables = xmlFile \ "tableDefines" \ "table"
    val tableName = (tables \ "tableName").text.toString.trim()
    val tb_items = (tables \ "items").text.toString.trim()
    val tb_itemsDescribe = (tables \ "itemsDescribe").text.toString.trim()

    tablesDefMap += ("tableName" -> tableName)
    tablesDefMap += ("items" -> tb_items)
    tablesDefMap += ("itemsDescribe" -> tb_itemsDescribe)
  }
}