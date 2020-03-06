import java.util

import org.apache.kudu.client.CreateTableOptions
import org.apache.kudu.spark.kudu.KuduContext
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}
import org.apache.spark.{SparkConf, SparkContext}

object KuduSparkTest1 {

  def main(args: Array[String]): Unit = {


    val conf: SparkConf = new SparkConf().setMaster("local[*]")
    val sc = new SparkContext(conf)
    val kuduContext = new KuduContext("",sc)
    val tableFileds =Array(StructField("",IntegerType,true))
    val arrayList = new util.ArrayList[String]()
    arrayList.add("id")
    val b=new CreateTableOptions().setNumReplicas(1).addHashPartitions(arrayList,3)
    kuduContext.createTable("",StructType(tableFileds),Seq("id"),b)

  }

}
