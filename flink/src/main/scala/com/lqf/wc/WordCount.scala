package com.lqf.wc

import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment}

object WordCount {


  def main(args: Array[String]): Unit = {

    val env: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    //从文件中读取数据
    val inputPath="/usr/local/flink/input.txt"
    val inputDS: DataSet[String] = env.readTextFile(inputPath)
    import org.apache.flink.api.scala._
    val wordCountDS: AggregateDataSet[(String, Int)] = inputDS.flatMap(_.split(" ")).map((_,1)).groupBy(0).sum(1)
    //打印输出
    wordCountDS.print()

  }

}
