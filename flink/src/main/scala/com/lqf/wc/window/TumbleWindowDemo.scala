package com.lqf.wc.window


import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.streaming.api.{CheckpointingMode, TimeCharacteristic}
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

object TumbleWindowDemo {

  def main(args: Array[String]): Unit = {

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime)
    env.enableCheckpointing(5000)
    env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)
    env.setParallelism(1)
    import org.apache.flink.api.scala._
    env.socketTextStream("",9900)
      .filter(_.nonEmpty)
      .flatMap(_.split(" "))
      .map((_,1))
    //根据tuple的第一个元素分组
      .keyBy(0)
    //滚动窗口，5秒滚动一次
      .timeWindow(Time.seconds(5))
    //聚合函数和窗口函数
//      .reduce((origin:(String,Long),input:(String,Long))=>(origin._1,origin._2+input._2),new ProcessWindowFunction[(String,Long),String,Tuple,TimeWindow] {
//        override def process(key: Tuple, context: Context, input: Iterable[(String, Long)], out: Collector[String]): Unit = {
//          // 这个其实就是分组的key,注意tuple1里面的泛型是key的类型,这里的类型一定要写对,不然是执行不了的
//          val k = key.asInstanceOf[Tuple1[String]].f0
//
//        }
//      })



  }

}
