package com.asiainfo.mix.log.impl

import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.Map
import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.streaming.dstream.DStream
import com.asiainfo.mix.xml.XmlProperiesAnalysis
import com.asiainfo.mix.streaming_log.LogTools

/**
 * @author surq
 * @since 2014.07.15
 * 汇总日志更新mysql 流处理
 */
class MergeLogAnalysis extends Serializable {

  /**
   * @param inputStream:log流数据<br>
   */
  def run(logtype: String, inputStream: DStream[Array[(String, String)]], logSteps: Int, outSeparator: String) = {

    val logPropertiesMaps = XmlProperiesAnalysis.getLogStructMap
    val tablesMap = XmlProperiesAnalysis.getTablesDefMap
    // rowkey 连接符
    val separator = "asiainfoMixSeparator"

    //输出为表结构样式　用
    val tbname = tablesMap("tableName")
    //输出为表结构样式　用
    val tbItems = tablesMap("items").split(",")

    inputStream.map(record => {
      val itemMap = record.toMap
      val rowKey = itemMap("rowKey")
      (rowKey, record)
    }).groupByKey.map(f => {

      // 创建db表结构并初始化
      val dbrecord = Map[String, String]()
      // 去除前取数据处理时，拼接的rowKey字段，此字段数据库中不存在
      val exceptItems = Array("rowKey")
      // 汇总所有类型log日志更新的字段
      f._2.foreach(record => {
        // 除联合rowkey之外要更新的字段item:[(k,v),(k,v)]
        val items = for (enum <- record if (enum._2 != "")) yield enum
        items.foreach(f => {
          if (!exceptItems.contains(f._1)) {
            // f._1: 除rowkey之外的字段
            dbrecord += ((f._1) -> (dbrecord.getOrElse(f._1, "0") + f._2).toString)
          }
        })
      })

      // 填充db表结构中的主key部分
      val keyarray = (f._1).split(separator)
      dbrecord += (("activity_id") -> keyarray(0))
      dbrecord += (("order_id") -> keyarray(1))
      dbrecord += (("material_id") -> keyarray(2))
      dbrecord += (("ad_id") -> keyarray(3))
      dbrecord += (("size_id") -> keyarray(4))
      dbrecord += (("area_id") -> keyarray(5))
      dbrecord += (("domain") -> keyarray(6))
      dbrecord += (("ad_pos_id") -> keyarray(7))
      val logdate = getlogtime(keyarray(8), logSteps)
      dbrecord += (("start_time") -> logdate._1)
      dbrecord += (("end_time") -> logdate._2)
      dbrecord += (("exchange_id") -> (if (keyarray(9).trim.isEmpty())"0" else keyarray(9)))
      dbrecord += (("app_id") -> (if (keyarray(10).trim.isEmpty())"0" else keyarray(10)))
      dbrecord += (("app_name") -> (if (keyarray(11).trim.isEmpty())"0" else keyarray(11)))
      dbrecord += (("exchange_app_cat_id") -> (if (keyarray(12).trim.isEmpty())"0" else keyarray(12)))

      // 输出，输出分隔符默认为"\t"
      val dataRow = for (item <- tbItems) yield (dbrecord.getOrElse(item, "0"))
      if (outSeparator == "") dataRow.mkString("\t") else dataRow.mkString(outSeparator)
    })
  }

  /**
   * 根据logtime的时间，取logtime所在的时间范围<br>
   */
  def getlogtime(logdate: String, logSteps: Int): Tuple2[String, String] = {
    // 补全起止时间格式
    var start_time = ((logdate.substring(10)).toInt * logSteps).toString
    start_time = "00" + start_time
    start_time = start_time.substring(start_time.length - 2)
    start_time = logdate.substring(0, 10) + start_time + "00"
    var end_time = ((((logdate.substring(10)).toInt + 1) * logSteps) - 1).toString
    end_time = "00" + end_time
    end_time = end_time.substring(end_time.length - 2)
    end_time = logdate.substring(0, 10) + end_time + "59"
    (start_time, end_time)
  }
}