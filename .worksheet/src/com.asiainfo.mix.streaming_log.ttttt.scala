package com.asiainfo.mix.streaming_log

object ttttt {;import org.scalaide.worksheet.runtime.library.WorksheetSupport._; def main(args: Array[String])=$execute{;$skip(311); 
  
  
    def valueToList(contentText: String) ={
    val separator =" "
    var data = contentText + separator + "MixSparkLogEndSeparator"
    val recodeList = data.split(separator)
    for (index <- 0 until recodeList.size-1)yield (recodeList(index))
  };System.out.println("""valueToList: (contentText: String)scala.collection.immutable.IndexedSeq[String]""");$skip(44); 
  valueToList("a b c d e")  foreach println;$skip(65); 
  
  def joins[DStream[String]] (d1:DStream[String]) ={
    
  };System.out.println("""joins: [DStream[String]](d1: DStream[String])Unit""")}
}
