package com.ks.sparkscala

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext

object CountWords {

  def main(args: Array[String]) {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val sc = new SparkContext("local[*]", "CountWords")


    val lines = sc.textFile("src/main/resources/book.txt")

    val words = lines.flatMap(_.split("\\W+")).map(_.toLowerCase)

    // not good way of doing as scala map memory
    //  val countWords = words.countByValue()

    // better way of doing the RDD way to keep it in cluster
    val countWordsRdd = words.map(x=> (x,1)).reduceByKey((x,y) => x+y)


    //countWords.foreach(println)
    println("---------------------")
    countWordsRdd.foreach(println)
  }

}
