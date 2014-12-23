package com.asiainfo.mix.streaming_log

import org.apache.spark.Logging
import scala.xml.XML
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.Seconds
import com.asiainfo.mix.xml.XmlProperiesAnalysis
import com.asiainfo.mix.log.impl.MergeLogAnalysis
import org.apache.hadoop.fs.Path
import org.apache.spark.SparkConf

/**
 * @author surq
 * @since 2014.12.17
 * 功能解介：<br>
 * 1、mix Log 处理入口类<br>
 * 2、解析logconf.xml中的各属性<br>
 * 3、logProperties:为欲处理log的相关信息<br>
 * 4、调用相应的日志实现流处理app<br>
 */
object LogAnalysisApp {

  def main(args: Array[String]): Unit = {
    // logconf.xml解析
    XmlProperiesAnalysis.getXmlProperies
    val commonPropsMap = XmlProperiesAnalysis.getCommonPropsMap
    val appName = commonPropsMap.getOrElse("appName", "Mix_Log_process")
    val interval = commonPropsMap.getOrElse("interval", "2").toInt
    val logSteps = commonPropsMap.getOrElse("logSteps", "15").toInt
    val logStructMap = XmlProperiesAnalysis.getLogStructMap
    val dataInput = XmlProperiesAnalysis.getDataInputMap
    val outputDataMap = XmlProperiesAnalysis.getOutputDataMap
    val outSeparator = outputDataMap("separator")

    // TODO
    //            val master = "local[2]"
    //            val ssc = new StreamingContext(master, appName, Seconds(interval))

    val sparkConf = new SparkConf().setAppName(appName)
    val ssc = new StreamingContext(sparkConf, Seconds(interval))

    // 生成各类文件流
    val dtreams = dataInput.map(f => {
      val logtype = f._1
      val inputPath = f._2("dir")
      val dstream = ssc.textFileStream(inputPath)
      (logtype, dstream)
    })

    // 数据与配置文件定义相拉链，变为[k,v]形式，并过滤字段个数不正确的数据
    val kvDstreams = logStructMap.map(log => {
      val logtype = log._1
      val mixLogMap = log._2
      val separator = mixLogMap("separator")
      val items = mixLogMap("items")
      val dstream = dtreams(logtype).filter(row => if (row.trim == "") false else true).map(row => {
        val data = row + separator + "mix"
        val recodeList = data.split(separator)
        val keyValueArray = (items.split(",")).zip(recodeList.take(recodeList.size - 1))
        keyValueArray
      }).filter(row => {
        val dataMap = row.toMap
        // 有误的log日志数据
        if (dataMap("loglen").trim != (dataMap.size).toString) {
          LogTools.mixWranning("日志类型[" + logtype + "]:字段个数有误［实际字段数=" + dataMap.size + "],data=" + row.mkString(","))
          false
        } else true
      })
      (logtype, dstream)
    })

    // 各类日志具体逻辑处理
    val dataDstreams = dataInput.map(input => {
      val logtype = input._1
      val appClass = input._2("appClass")
      val structMap = logStructMap(logtype)
      val clz = Class.forName(appClass)
      val constructors = clz.getConstructors()
      val constructor = constructors(0).newInstance()
      val outputStream = constructor.asInstanceOf[StreamAction].run(logtype, kvDstreams(logtype), logSteps)
      outputStream
    }).toArray

    // 多日志合并更新mysql
    val unionStreams = dataDstreams.reduce(_ union _)
    val mergeLogAnalysis = new MergeLogAnalysis()
    val outputStream = mergeLogAnalysis.run("merge", unionStreams, logSteps, outSeparator)

    outputStream.foreachRDD(rdd => {
      if (rdd.partitions.size > 0) {
        val dataList = rdd.collect
        if (dataList.size > 0) {
          val fileWriter = LogTools.mixLogWriter(outputDataMap.toArray)
          dataList.foreach(row => fileWriter.write(row + System.getProperty("line.separator")))
          fileWriter.flush
          fileWriter.close()
        }
      }
    })
    ssc.start()
    ssc.awaitTermination()
  }
}