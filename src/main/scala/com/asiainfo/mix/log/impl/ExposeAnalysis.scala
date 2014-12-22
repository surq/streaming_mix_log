package com.asiainfo.mix.log.impl

import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.streaming.dstream.DStream
import com.asiainfo.mix.streaming_log.LogTools
import com.asiainfo.mix.streaming_log.StreamAction
import com.asiainfo.mix.xml.XmlProperiesAnalysis

/**
 * @author surq
 * @since 2014.07.15
 * 曝光日志 流处理
 */
class ExposeAnalysis extends StreamAction with Serializable {

  /**
   * @param inputStream:log流数据<br>
   */
  override def run(logtype: String, inputStream: DStream[Array[(String, String)]], logSteps: Int): DStream[Array[(String, String)]] = {
    printInfo(this.getClass(), "ExposeAnalysis is running!")

    val tablesMap = XmlProperiesAnalysis.getTablesDefMap
    val logPropertiesMaps = XmlProperiesAnalysis.getLogStructMap
    val logPropertiesMap = logPropertiesMaps(logtype)

    // log数据主key
    val keyItems = logPropertiesMap("rowKey").split(",")
    //输出为表结构样式　用
    val tbItems = tablesMap("items").split(",")
    // rowkey 连接符
    val separator = "asiainfoMixSeparator"

    inputStream.map(record => {
      val itemMap = record.toMap
      printDebug(this.getClass(), "source DStream:" + itemMap.mkString(","))
      val keyMap = (for { key <- keyItems } yield (key, itemMap(key))).toMap
      (rowKeyEditor(keyMap, logSteps, separator), record)
    }).groupByKey.map(f => {

      val count = f._2.size
      val sumCost = for { f <- f._2; val map = f.toMap } yield (map("cost").toFloat)

      // 创建db表结构并初始化
      var dbrecord = Map[String, String]()
      // 流计算
      dbrecord += (("bid_success_cnt") -> count.toString)
      dbrecord += (("expose_cnt") -> count.toString)
      dbrecord += (("cost") -> (sumCost.sum).toString)

      // 顺列流字段为db表结构字段,第一个字段rowKey
      var mesgae = LogTools.setTBSeq(f._1, tbItems, dbrecord)
      mesgae.toArray
    })
  }

  /**
   * 根据曝光日志和最终要更新的mysql表的主键<br>
   * 编辑曝光日志的rowkey<br>
   * logSteps:设定的log时间分片时长<br>
   * rowkey (表中的顺序): 活动ID+订单ID+素材ID+广告ID+尺寸ID+地域ID+媒体+广告位ID+日志时间<br>
   */
  def rowKeyEditor(keyMap: Map[String, String], logSteps: Int, separator: String): String = {
    //编辑曝光日志 主key    
    //日志时间 logtime
    //广告id	adid
    //订单id	orderid
    //活动id	activityid
    //referer	referer
    //地域id	areaid
    //尺寸id	sizeid
    //广告位id	playlocation
    //素材ID与广告ID相同
    val referer = LogTools.getDomain(keyMap("referer"))
    val log_time = LogTools.timeConversion_H(keyMap("logtime")) + LogTools.timeFlg(keyMap("logtime"), logSteps)
    keyMap("activityid") + separator +
      keyMap("orderid") + separator +
      keyMap("adid") + separator +
      keyMap("adid") + separator +
      keyMap("sizeid") + separator +
      keyMap("areaid") + separator +
      referer + separator +
      keyMap("playlocation") + separator +
      log_time
  }
}