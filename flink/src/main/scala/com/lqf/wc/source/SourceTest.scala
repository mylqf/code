package com.lqf.wc.source

import java.util.Properties

import com.lqf.wc.window.SensorReading
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.util.serialization.SimpleStringSchema

import scala.util.Random

object SourceTest {


  def main(args: Array[String]): Unit = {

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    import org.apache.flink.api.scala._
    //从集合中读取数据
    val stream1=env.fromCollection(List(
      SensorReading("sensor_1", 1547718199, 35.80018327300259),
        SensorReading("sensor_6", 1547718201, 15.402984393403084),
        SensorReading("sensor_7", 1547718202, 6.720945201171228),
        SensorReading("sensor_10", 1547718205, 38.101067604893444)
    ))

    //从文件中读取数据
    val stream2=env.readTextFile("D:\\code\\flink\\src\\main\\resources\\sensor.txt")

    //从kafka读取数据
    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "linux01:9092")
    properties.setProperty("group.id", "consumer-group")
    properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("auto.offset.reset", "latest")

    //val stream3=env.addSource(new FlinkKafkaConsumer010[String]("sensor",new SimpleStringSchema(), properties))

    //自定义数据源
    val stream4=env.addSource(new SensorSourcece())

    //sink输出
    stream4.print("stream4")

    env.execute("source api test")

  }
}

class SensorSource() extends SourceFunction[SensorReading]{

  var running:Boolean=true

  override def run(sourceContext: SourceFunction.SourceContext[SensorReading]): Unit = {
    //创建一个随机数发生器
    val random = new Random()

    var curTemp=1.to(10).map(
      i=>("sensor_"+i,60+random.nextGaussian()*20)
    )

    //无限循环生成流数据，除非被cancel
    while(running){
      //更新温度值
      curTemp=curTemp.map(
        t=>(t._1,t._2+random.nextGaussian())
      )
      //获取当前的时间戳
      val curTime: Long = System.currentTimeMillis()
      //包装成SensorReading，输出
      curTemp.foreach(
        t=>sourceContext.collect(SensorReading(t._1,curTime,t._2))
      )
      //间隔100ms
      Thread.sleep(100)
    }

  }

  override def cancel(): Unit = running=false
}
