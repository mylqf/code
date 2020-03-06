package com.lqf.wc.window

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.{AssignerWithPeriodicWatermarks, AssignerWithPunctuatedWatermarks}
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.time.Time

object WindowTest {

  def main(args: Array[String]): Unit = {


    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    //设置事件世间
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
   // env.getConfig.setAutoWatermarkInterval(100L)

    val inputStream: DataStream[String] =
      //env.readTextFile("D:\\code\\flink\\src\\main\\resources\\sensor.txt")
      env.socketTextStream("linux01",7777)
    import  org.apache.flink.api.scala._
    val dataStream=inputStream.map(data=>{
      val dataArray: Array[String] = data.split(",")
      SensorReading(dataArray(0).trim,
      dataArray(1).trim.toLong,dataArray(2).trim.toDouble)
    })
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[SensorReading](Time.seconds(1)) {
        override def extractTimestamp(element: SensorReading): Long = element.timestamp*1000L
      })
   val minTempPerWindowsStream=dataStream
      .map(data=>(data.id,data.temperature))
      .keyBy(_._1)
      .timeWindow(Time.seconds(10))
      .reduce((result,data)=>(data._1,result._2.min(data._2))) //统计10s内的最低温度


    minTempPerWindowsStream.print("min temp")
    dataStream.print("input data")

    env.execute("window api test")

  }

}


class MyAssigner()  extends AssignerWithPeriodicWatermarks[SensorReading]{

  //定义固定延迟为3秒
  val bound:Long=3*1000L
  //定义当前收到的最大的时间戳
  var maxTs: Long = Long.MinValue

  override def getCurrentWatermark: Watermark = {
    new Watermark(maxTs-bound)
  }
  override def extractTimestamp(element: SensorReading, l: Long): Long = {
    maxTs=maxTs.max(element.timestamp*1000L)
    element.timestamp*1000L
  }
}


class MyAssigner2() extends AssignerWithPunctuatedWatermarks[SensorReading]{

  val bound:Long=1000L

  override def checkAndGetNextWatermark(lastElement: SensorReading, extractedTimestamp: Long): Watermark = {
    if(lastElement.id=="sensor_1"){
      new Watermark(extractedTimestamp-bound)
    }else{
      null
    }
  }

  override def extractTimestamp(element: SensorReading, previousElementTimestamp: Long): Long = {
    element.timestamp*1000L
  }

}

case class SensorReading(id: String,timestamp:Long,temperature:Double)
