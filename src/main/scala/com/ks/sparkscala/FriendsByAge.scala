package com.ks.sparkscala

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext

object FriendsByAge {

  def parseLines(line: String): (Int, Int) = {
    val fields = line.split(",");
    val age = fields(2).toInt
    val count = fields(3).toInt
    (age, count)
  }


  def main(args: Array[String]) {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val sc = new SparkContext("local[*]", "FriendsByAge")


    val lines = sc.textFile("src/main/resources/fakefriends.csv")

    val rdd = lines.map(parseLines)

    val totalByAge = rdd.mapValues(x => (x,1)).reduceByKey((x,y) => (x._1 + y._1, x._2 + y._2))

    val averagebyAge = totalByAge.mapValues(x => x._1 / x._2)

    val results = averagebyAge.collect()

    results.sorted.foreach(println)
    println("----------------------------------------")
    println("Age that has max no. of frnds: " + results.maxBy(_._2)._1)
    println("Age that has min no. of frnds: " + results.minBy(_._2)._1)

  }
}
