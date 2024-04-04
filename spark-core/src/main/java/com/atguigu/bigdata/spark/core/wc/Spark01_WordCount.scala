package com.atguigu.bigdata.spark.core.wc

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark01_WordCount {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("WordCount")
    val sc = new SparkContext(conf)


    //读取文件每个line
    val lines = sc.textFile(path = "datas");
    // 读取文件数据
   // val fileRDD: RDD[String] = sc.textFile("input/word.txt")
    // 将文件中的数据进行分词
   // val wordRDD: RDD[String] = fileRDD.flatMap(_.split(" "))

    // split to each word
    val words = lines.flatMap(_.split(" "))


    // 分组
    val wordGroup = words.groupBy(word => word)


    val wordToCount = wordGroup.map {
      case (word, list) => {
        (word, list.size)
      }
    }

    val array = wordToCount.collect()

    array.foreach(println)

    sc.stop()
  }

}
