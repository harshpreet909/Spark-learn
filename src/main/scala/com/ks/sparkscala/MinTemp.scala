package com.ks.sparkscala

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext

object MinTemp {

  def parseLine(line: String): (Int, String, Float) ={
    val fields = line.split(",")
    val stationId = fields(1).toInt
    val entryType = fields(2)
    val temperature = fields(3).toFloat * 0.1f * (9.0f / 5.0f) + 32.0f
    (stationId, entryType, temperature)
  }

  def min(x:Float , y:Float):Float = {
    if(x>y)
      y
    x
  }

  def main(args: Array[String]) {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val sc = new SparkContext("local[*]", "FriendsByAge")


    val lines = sc.textFile("src/main/resources/1800.csv")

    val parsedLine = lines.map(parseLine)

    val minTempRecord = parsedLine.filter(x=> x._2 == "TMIN").map(x=> (x._1, x._3.toFloat))

    val minTempByStation = minTempRecord.reduceByKey((x,y) => min(x,y))

    val results = minTempByStation.collect()

    results.sorted.foreach(println)

    println("-------------------------------------------------")
    println(s"Min temperature of station " + results.minBy(_._2)._1 + "is: "  + results.minBy(_._2)._2)
    println(s"Max temperature of station " + results.maxBy(_._2)._1 + "is: "  + results.maxBy(_._2)._2)
  }
}
