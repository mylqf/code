package com.lqf.wc

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}

object StreamWordCount {

  def main(args: Array[String]): Unit = {

    //从外部系统命令中获取参数
    val params: ParameterTool = ParameterTool.fromArgs(args)
    val host: String = params.get("host")
    val port: Int = params.getInt("port")

    //创建流处理环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    //接受socket文本流
    val testStream: DataStream[String] = env.socketTextStream(host,port)
    import org.apache.flink.api.scala._
    val dataStream=testStream.flatMap(_.split("\\s")).filter(_.nonEmpty)
      .map((_,1)).keyBy(0).sum(1)

    dataStream.print().setParallelism(1)

    //启动executor，执行任务
    env.execute("Socker stream word Count")



  }

}
