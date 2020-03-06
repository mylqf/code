package com.lqf.wc.sink

import java.util

import com.lqf.wc.window.SensorReading
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}

object EsSinkTest {

  private val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
  env.setParallelism(1)

  //source
  private val inputStream: DataStream[String] = env.readTextFile("D:\\code\\flink\\src\\main\\resources\\sensor.txt")
  //transform
  import org.apache.flink.api.scala._
  val dataStream=inputStream.map(
    data=>{
      val dataArray: Array[String] = data.split(",")
      SensorReading( dataArray(0).trim, dataArray(1).trim.toLong, dataArray(2).trim.toDouble)
    }
  )
//  private val httpPosts = new util.ArrayList[HttpHost]()
//  httpPosts.add(new HttpHost("linux01",9200))
//
//  //创建一个esSink的Builder
//  new ElasticSearchSink.Builder[SensorReading](
//    httpPosts,
//    new ElasticSearchSinkFunction[SensorReading]{
//
//    }
//  )

}
