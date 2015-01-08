package com.asiainfo.mix.streaming_log

/**
 * 维度编辑<br>
 * @author surq
 * @since 2014.12.24
 * 取得联合rowkey：
 * activity_id+order_id+material_id+ad_id+size_id+area_id+ domain +
 * ad_pos_id+log_time +exchange_id+app_id+app_name+exchange_app_cat_id
 */
object DimensionEditor extends Serializable {

  def getUnionKey(paramList: Tuple4[String, Map[String, String], Int, String]) = {
    val (logType, keyMap, logSteps, separator) = paramList
    logType match {
      case "bid" => getBidRowKeyEditor(keyMap, logSteps, separator)
      case "click" => getClickRowKeyEditor(keyMap, logSteps, separator)
      case "expose" => getExposeRowKeyEditor(keyMap, logSteps, separator)
      case "arrive" => getArriveChangeRowKeyEditor(keyMap, logSteps, separator)
      case "change" => getArriveChangeRowKeyEditor(keyMap, logSteps, separator)
      case _  => println("无此类型日志，请检查["+ logType +"]拼写");""
    }
  }

  /**
   * 联合rowkey:
   *
   */
  val ox003: Char = 3

  /**
   * 根据到达日志和最终要更新的mysql表的主键<br>
   * 编辑到达日志的rowkey<br>
   * logSteps:设定的log时间分片时长<br>
   * rowkey (表中的顺序): 活动ID+订单ID+素材ID+广告ID+尺寸ID+地域ID+媒体+
   * 广告位ID+日志时间+exchange_id+app_id+app_name+exchange_app_cat_id<br>
   */
  private def getArriveChangeRowKeyEditor(keyMap: Map[String, String], logSteps: Int, separator: String): String = {
    //到达日志 主key		
    //日志时间	log_time
    //referer	media_url
    //订单id	id	●
    val id = keyMap("order")
    // （time_domain_sizeid_areaid_slotid_cid_oid_adid）
    val idList = LogTools.splitArray(id, ox003.toString, 10)
    // 活动ID
    val activity_id = idList(5)
    //订单ID
    val order_id = idList(6)
    // 素材ID ad_id和material_id相同
    val material_id = idList(7)
    // 广告ID
    val ad_id = idList(7)
    // 尺寸ID
    val size_id = idList(2)
    //地域ID	area_id
    val area_id = idList(3)
    //URL	media_url
    val domain = idList(1)
    // 广告位置ID
    val ad_pos_id = idList(4)
    // 广告交易平台ID	
    val exchange_id = "0"
    // app_id
    val app_id = "0"
    // app名称
    val app_name = "0"
    // app分类ID
    val exchange_app_cat_id = "0"

    val log_time = LogTools.timeConversion_H(keyMap("logtime")) + LogTools.timeFlg(keyMap("logtime"), logSteps)
    activity_id + separator +
      order_id + separator +
      material_id + separator +
      ad_id + separator +
      size_id + separator +
      area_id + separator +
      domain + separator +
      ad_pos_id + separator +
      log_time + separator +
      exchange_id + separator +
      app_id + separator +
      app_name + separator +
      exchange_app_cat_id
  }

  /**
   * bid
   * 根据竞价日志和最终要更新的mysql表的主键<br>
   * 编辑竞价日志的rowkey<br>
   * logSpace:设定的log时间分片时长<br>
   * rowkey (表中的顺序): 活动ID+订单ID+素材ID+广告ID+尺寸ID+地域ID+媒体+
   * 广告位ID+日志时间+exchange_id+app_id+app_name+exchange_app_cat_id<br>
   */
  private def getBidRowKeyEditor(keyMap: Map[String, String], logSteps: Int, separator: String): String = {
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
    val material_id = infoList(4)
    // 广告ID
    val ad_id = infoList(0)
    // 尺寸ID
    val size_id = infoList(6)

    // 18(slotId_size_广告位ID_landpage_lowprice)
    val ad_pos_info = keyMap("ad_pos_info")
    val posinfoList = LogTools.splitArray(ad_pos_info, ox004.toString, 5)
    // 广告位ID
    val ad_pos_id = posinfoList(1)
    //地域ID	area_id
    val area_id = keyMap("area_id")
    //URL	media_url
    val domain = LogTools.getDomain(keyMap("url"))
    // 广告交易平台ID	
    var exchange_id = "0"
    if (!(keyMap("exchange_id").trim.isEmpty)) exchange_id = keyMap("exchange_id")
    // app_id
    var app_id = "0"
    if (!(keyMap("app_id").trim.isEmpty)) app_id = keyMap("app_id")
    // app名称
    var app_name = "0"
    if (!(keyMap("app_name").trim.isEmpty)) app_name = keyMap("app_name")
    // app分类ID
    var exchange_app_cat_id = "0"
    if (!(keyMap("pageid").trim.isEmpty)) exchange_app_cat_id = keyMap("pageid")

    val log_time = LogTools.timeConversion_H(keyMap("logtime")) + LogTools.timeFlg(keyMap("logtime"), logSteps)
    activity_id + separator +
      order_id + separator +
      material_id + separator +
      ad_id + separator +
      size_id + separator +
      area_id + separator +
      domain + separator +
      ad_pos_id + separator +
      log_time + separator +
      exchange_id + separator +
      app_id + separator +
      app_name + separator +
      exchange_app_cat_id
  }

  /**
   * 根据点击日志和最终要更新的mysql表的主键<br>
   * 编辑点击日志的rowkey<br>
   * logSteps:设定的log时间分片时长<br>
   * rowkey (表中的顺序): 活动ID+订单ID+素材ID+广告ID+尺寸ID+地域ID+媒体+广告位ID+
   * 日志时间+exchange_id+app_id+app_name+exchange_app_cat_id<br>
   */
  private def getClickRowKeyEditor(keyMap: Map[String, String], logSteps: Int, separator: String): String = {
    //点击日志 主key		
    //日志时间	logtime
    //广告id	adid	adid和materialid相同
    //订单id	orderid
    //活动id	activityid	
    //referer	url	对应dsp_report_data中的media
    //地域id	areaid	
    //尺寸id	sizeid	
    //广告位id	playlocation
    //素材ID与广告ID相同
    val referer = LogTools.getDomain(keyMap("referer"))
    val log_time = LogTools.timeConversion_H(keyMap("logtime")) + LogTools.timeFlg(keyMap("logtime"), logSteps)
    // 广告交易平台ID	
    var streamsrc = "0"
    if (!(keyMap("streamsrc").trim.isEmpty)) streamsrc = keyMap("streamsrc")
    // app_id
    var app_id = "0"
    if (!(keyMap("app_id").trim.isEmpty)) app_id = keyMap("app_id")
    // app名称
    var app_name = "0"
    if (!(keyMap("app_name").trim.isEmpty)) app_name = keyMap("app_name")
    // app分类ID
    var exchange_app_cat_id = "0"
    if (!(keyMap("pageid").trim.isEmpty)) exchange_app_cat_id = keyMap("pageid")

    keyMap("activityid") + separator +
      keyMap("orderid") + separator +
      keyMap("adid") + separator +
      keyMap("adid") + separator +
      keyMap("sizeid") + separator +
      keyMap("areaid") + separator +
      referer + separator +
      keyMap("playlocation") + separator +
      log_time + separator +
      streamsrc + separator +
      app_id + separator +
      app_name + separator +
      exchange_app_cat_id
  }

  /**
   * expose
   * 根据曝光日志和最终要更新的mysql表的主键<br>
   * 编辑曝光日志的rowkey<br>
   * logSteps:设定的log时间分片时长<br>
   * rowkey (表中的顺序): 活动ID+订单ID+素材ID+广告ID+尺寸ID+地域ID+媒体+广告位ID+日志时间<br>
   */
  private def getExposeRowKeyEditor(keyMap: Map[String, String], logSteps: Int, separator: String): String = {
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
    // 广告交易平台ID	
    var streamsrc = "0"
    if (!(keyMap("streamsrc").trim.isEmpty)) streamsrc = keyMap("streamsrc")
    // app_id
    var app_id = "0"
    if (!(keyMap("app_id").trim.isEmpty)) app_id = keyMap("app_id")
    // app名称
    var app_name = "0"
    if (!(keyMap("app_name").trim.isEmpty)) app_name = keyMap("app_name")
    // app分类ID
    var exchange_app_cat_id = "0"
    if (!(keyMap("pageid").trim.isEmpty)) exchange_app_cat_id = keyMap("pageid")
    keyMap("activityid") + separator +
      keyMap("orderid") + separator +
      keyMap("adid") + separator +
      keyMap("adid") + separator +
      keyMap("sizeid") + separator +
      keyMap("areaid") + separator +
      referer + separator +
      keyMap("playlocation") + separator +
      log_time + separator +
      streamsrc + separator +
      app_id + separator +
      app_name + separator +
      exchange_app_cat_id
  }
}