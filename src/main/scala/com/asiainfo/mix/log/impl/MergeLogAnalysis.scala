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
    val tbItems = tablesMap("items").split(",")

    inputStream.map(record => {
      var itemMap = record.toMap
      val rowKey = itemMap("rowKey")
      // 统计字段排除rowKey
      itemMap -= "rowKey"
      (rowKey, itemMap.toArray)
    }).groupByKey.map(f => {
      // 创建db表结构并初始化"0"
      val dbrecord = Map[String, String]()
      tbItems.map(item => {
        dbrecord += (item -> "0")
      })

      // 汇总所有类型log日志更新的字段
      val itemList = f._2.flatMap(f => f)
      itemList.foreach(iterm => {
        dbrecord += ((iterm._1) -> (dbrecord(iterm._1).toLong + (iterm._2).toLong).toString)
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
      val logdate = LogTools.getlogtime(keyarray(8), logSteps)
      dbrecord += (("start_time") -> logdate._1)
      dbrecord += (("end_time") -> logdate._2)
      dbrecord += (("exchange_id") -> keyarray(9))
      dbrecord += (("app_id") -> keyarray(10))
      dbrecord += (("app_name") -> keyarray(11))
      dbrecord += (("exchange_app_cat_id") -> keyarray(12))

      // 输出，输出分隔符默认为"\t"
      val dataRow = for (item <- tbItems) yield (dbrecord(item))
      if (outSeparator == "") dataRow.mkString("\t") else dataRow.mkString(outSeparator)
    })
  }
}