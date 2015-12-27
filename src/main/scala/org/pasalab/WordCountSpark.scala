package org.pasalab

import org.apache.spark.{SparkContext, SparkConf}


/**
  * Created by dell on 2015/12/27.
  */
object WordCountSpark {
  def main(args: Array[String]) {
    val conf = new SparkConf()
    val sc = new SparkContext()
    val file = sc.textFile("hdfs://")
    val counts = file.flatMap(line => line.split(" ")) //分词
    .map(word => (word, 1)) //对应mapper的工作
      .reduceByKey(_ + _)
    //相同key的不同value之间进行”+”运算
    counts.saveAsTextFile("hdfs://...")
  }


}
