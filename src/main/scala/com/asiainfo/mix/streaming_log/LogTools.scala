package com.asiainfo.mix.streaming_log

import java.sql.DriverManager
import org.apache.spark.Logging
import scala.collection.mutable.ArrayBuffer
import java.sql.SQLException
import java.sql.Connection
import java.io.FileWriter
import java.util.Calendar
import java.io.File

object LogTools extends Logging with Serializable {

  /**
   * 创建日志写文件，写入器FileWriter
   */
  def mixLogWriter(outPutArray: Array[(String, String)]): FileWriter = {
    val outputDataMap = outPutArray.toMap
    val path = outputDataMap("saveAsFilePath")
    val extension = outputDataMap("extension")

    val dataDir = new File(path)
    if (!dataDir.isDirectory()) dataDir.mkdirs()

    val dateFormat = new java.text.SimpleDateFormat("yyyyMMddHHmm")
    val fileName = dateFormat.format(Calendar.getInstance().getTimeInMillis())
    val dataFilePath = path + System.getProperty("file.separator") + fileName + extension
    new FileWriter(dataFilePath, true)
  }

  /**
   * 　返回log所在的时间段
   */
  def timeFlg(unix_time_str: String, split: Int): String = {
    val dateFormat = new java.text.SimpleDateFormat("mm")
    val mmstr = dateFormat.format(unix_time_str.toLong);
    val flg = (mmstr.toInt) / (split)
    flg.toString
  }

  /**
   * 　把长整型的时间转成"年月日时分"
   */
  def timeConversion_H(unix_time_str: String): String = {
    var datatime = ""
    if (unix_time_str.length < 13) {
      datatime = unix_time_str + "0000000000000"
      datatime = datatime.substring(0, 13)
    } else datatime = unix_time_str
    val dateFormat = new java.text.SimpleDateFormat("yyyyMMddHH")
    dateFormat.format(datatime.toLong);
  }

  /**
   * 　把长整型的时间转成"年月日"
   */
  def timeConversion_D(unix_time_str: String): String = {
    var datatime = ""
    if (unix_time_str.length < 13) {
      datatime = unix_time_str + "0000000000000"
      datatime = datatime.substring(0, 13)
    } else datatime = unix_time_str
    val dateFormat = new java.text.SimpleDateFormat("yyyy-MM-dd")
    dateFormat.format(datatime.toLong);
  }

  /**
   * 　提取出完整域名
   */
  def getDomain(http_str: String): String = {
    var urldomain = ""
    var index = http_str.indexOf("://")
    if (index != -1) {
      urldomain = http_str.substring(index + 3)
      index = urldomain.indexOf("/")
      if (index != -1) {
        urldomain = urldomain.substring(0, index)
      }
    }
    urldomain
  }

  /**
   * 把流数据变为mysql表中的结构（额外附加一个rowkey字段）<br>
   * @param tbItems:mysql表字段列表<br>
   * @param dbrecord:流计算的结果字段<br>
   * @param kafkaseparator:返回字符串时，字段拼接用到的连接符<br>
   *
   */
  def setTBSeq(rowkey: String, tbItems: Array[String], dbrecord: Map[String, String]): ArrayBuffer[(String, String)] = {

    //变为db的字段的顺序
    val dbrecordArray = ArrayBuffer[(String, String)]()
    // 附加字段rowkey 后期merge用
    dbrecordArray += (("rowKey", rowkey))
    // db的字段加载
    tbItems.foreach(item => (dbrecordArray += ((item, dbrecord.getOrElse(item, "")))))
    dbrecordArray
  }

  /**
   * log 打印
   */
  def mixDebug(msg: String) = { log.debug(msg) }
  def mixInfo(msg: String) = { log.info(msg) }
  def mixWranning(msg: String) = { log.warn(msg) }
  def mixError(msg: String) = { log.error(msg) }
  def mixError(msg: String, u: Unit) = { log.error(msg, u) }

  /**
   * 字符串分隔为定长的array<br>
   * @param textItem:要分隔的字符串<br>
   * @param separator:分隔符<br>
   * @param length:返回目标长度的数组<br>
   * 例：
   * str=“abc————”
   * 返回（"abc","","","",""）
   */
  def splitArray(textItem: String, separator: String, length: Int): Array[String] = {
    val srcarray = textItem.split(separator)
    val odeArray = ArrayBuffer[String]()
    odeArray ++= srcarray
    (srcarray.size until length).map(i => {
      odeArray += ""
    })
    odeArray.toArray
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