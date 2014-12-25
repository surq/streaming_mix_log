package com.asiainfo.mix.log.impl

import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.streaming.dstream.DStream
import com.asiainfo.mix.streaming_log.LogTools
import com.asiainfo.mix.streaming_log.StreamAction
import com.asiainfo.mix.xml.XmlProperiesAnalysis
import com.asiainfo.mix.streaming_log.DimensionEditor

/**
 * @author surq
 * @since 2014.07.15
 * 竞价日志 流处理
 */
class BidAnalysis extends StreamAction with Serializable {

  /**
   * @param inputStream:log流数据<br>
   */
  override def run(logtype: String, inputStream: DStream[Array[(String, String)]], logSteps: Int): DStream[Array[(String, String)]] = {
    printInfo(this.getClass(), "BitAnalysis is running!")

    val logPropertiesMaps = XmlProperiesAnalysis.getLogStructMap
    val logPropertiesMap = logPropertiesMaps(logtype)
    // log数据主key
    val keyItems = logPropertiesMap("rowKey").split(",")

    // rowkey 连接符
    val separator = "asiainfoMixSeparator"

    inputStream.map(record => {
      val itemMap = record.toMap
      val keyMap = (for { key <- keyItems } yield (key, itemMap(key))).toMap
      (DimensionEditor.getUnionKey("bid",keyMap, logSteps, separator), record)
    }).groupByKey.map(f => {
      val count = f._2.size
      // 创建db表结构并初始化
      var dbrecord = Map[String, String]()
      // 流计算
      dbrecord += (("bid_cnt") -> count.toString)
      // rowKey: 多维度联合主键
      dbrecord += (("rowKey", f._1))
      dbrecord.toArray
    })
  }
}