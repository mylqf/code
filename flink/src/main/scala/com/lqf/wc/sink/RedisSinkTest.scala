package com.lqf.wc.sink

import com.lqf.wc.window.SensorReading
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

object RedisSinkTest {
  def main(args: Array[String]): Unit = {

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    //Source
    val inputStream=env.readTextFile("D:\\code\\flink\\src\\main\\resources\\sensor.txt")
    import org.apache.flink.api.scala._
    //transform
    val dataStream=inputStream.map(
      data=>{
        val dataArray: Array[String] = data.split(",")
        SensorReading(dataArray(0).trim,dataArray(1).trim.toLong,dataArray(2).trim.toDouble)
      }
    )

//    new FlinkJedisPoolConfig.Builder

  }

}
