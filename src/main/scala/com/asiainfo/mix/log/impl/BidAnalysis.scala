package com.asiainfo.mix.log.impl

import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.streaming.dstream.DStream
import com.asiainfo.mix.streaming_log.LogTools
import com.asiainfo.mix.streaming_log.StreamAction
import com.asiainfo.mix.xml.XmlProperiesAnalysis

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
    val tablesMap = XmlProperiesAnalysis.getTablesDefMap
    //输出为表结构样式　用
    val tbItems = tablesMap("items").split(",")
    
    // rowkey 连接符
    val separator = "asiainfoMixSeparator"
      
    inputStream.map(record => {
      val itemMap = record.toMap
      val keyMap = (for { key <- keyItems } yield (key, itemMap(key))).toMap
      (rowKeyEditor(keyMap, logSteps, separator), record)
    }).groupByKey.map(f => {
      val count = f._2.size
      // 创建db表结构并初始化
      var dbrecord = Map[String, String]()
      // 流计算
      dbrecord += (("bid_cnt") -> count.toString)
      // 顺列流字段为db表结构字段,第一个字段rowKey
      var mesgae = LogTools.setTBSeq(f._1, tbItems, dbrecord)
      mesgae.toArray
    })
  }
  
  /**
   * 根据竞价日志和最终要更新的mysql表的主键<br>
   * 编辑竞价日志的rowkey<br>
   * logSpace:设定的log时间分片时长<br>
   * rowkey (表中的顺序): 活动ID+订单ID+素材ID+广告ID+尺寸ID+地域ID+媒体+广告位ID+日志时间<br>
   */
  def rowKeyEditor(keyMap: Map[String, String], logSteps: Int, separator: String): String = {
    //竞价日志 主key		
    //请求日期/时间	log_time
    //地域ID	area_id
    //URL	media_url 提取出完整域名
    //参与竞价广告信息	bitted_ad_info 包含多个子字段：(广告ID ad_id、 ad_id和material_id相同、
    //订单ID order_id、活动ID activity_id、广告位ID ad_pos_id、出价 bit_price、sizeid size_id  
    val bitted_ad_infoList = keyMap("joinbid_ad_info")
    val ox003: Char = 3
    val bitted_ad_info = (bitted_ad_infoList.split(ox003))(0)
    val ox004: Char = 4
    // 20(adId_orderId_cId_slotId_price_sizeId)
    val infoList = LogTools.splitArray(bitted_ad_info, ox004.toString, 6)

    //活动ID	●	20(adId_orderId_活动ID_slotId_price_sizeId)
    //订单ID	●	20(adId_订单ID_cId_slotId_price_sizeId)
    //素材ID	●	20(素材ID_orderId_cId_slotId_price_sizeId)
    //广告ID	●	20(广告ID_orderId_cId_slotId_price_sizeId)
    //尺寸ID	●	20(adId_orderId_cId_slotId_price_尺寸ID)

    // 活动ID
    val activity_id = infoList(2)
    //订单ID
    val order_id = infoList(1)
    // 素材ID ad_id和material_id相同
    val material_id = infoList(0)
    // 广告ID
    val ad_id = infoList(0)
    // 尺寸ID
    val size_id = infoList(5)

    // 18(slotId_size_广告位ID_landpage_lowprice)
    val ad_pos_info = keyMap("ad_pos_info")
    val posinfoList = LogTools.splitArray(ad_pos_info, ox004.toString, 5)
    // 广告位ID
    val ad_pos_id = posinfoList(1)
    
    //地域ID	area_id
    val area_id = keyMap("area_id")
    //URL	media_url
    val media_url = LogTools.getDomain(keyMap("url"))

    val log_time = LogTools.timeConversion_H(keyMap("logtime")) + LogTools.timeFlg(keyMap("logtime"), logSteps)
    activity_id + separator +
      order_id + separator +
      material_id + separator +
      ad_id + separator +
      size_id + separator +
      area_id + separator +
      media_url + separator +
      ad_pos_id + separator +
      log_time
  }
}