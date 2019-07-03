package com.ks.sparkscala

import org.apache.spark._
import org.apache.log4j._
import scala.io.Source
import java.nio.charset.CodingErrorAction
import scala.io.Codec



object PopularMovie {

  def parseLines(line:String): (Int) = {
    line.split("\t")(1).toInt
  }

  /** Load up a Map of movie IDs to movie names. */
  def loadMovieNames() : Map[Int, String] = {

    // Handle character encoding issues:
    implicit val codec = Codec("UTF-8")
    codec.onMalformedInput(CodingErrorAction.REPLACE)
    codec.onUnmappableCharacter(CodingErrorAction.REPLACE)

    // Create a Map of Ints to Strings, and populate it from u.item.
    var movieNames:Map[Int, String] = Map()

    val lines = Source.fromFile("src/main/resources/data/u.item").getLines()
    for (line <- lines) {
      var fields = line.split('|')
      if (fields.length > 1) {
        movieNames += (fields(0).toInt -> fields(1))
      }
    }

    return movieNames
  }

  def main(args: Array[String]) {

    Logger.getLogger("org").setLevel(Level.ERROR)

    val sc = new SparkContext("local[1]", "CountWords")

    val nameDict = sc.broadcast(loadMovieNames)

    val lines = sc.textFile("src/main/resources/data/u.data")

    val movieIds = lines.map(parseLines(_))

    val movieMap = movieIds.map(x=> (x,1))

    val movieCounts = movieMap.reduceByKey((x,y) => x+y)

    val flipped = movieCounts.map(x=> (x._2, x._1)).sortByKey()

    val sortedMoviesWithName = flipped.map(x=> (nameDict.value(x._2),x._1))

    sortedMoviesWithName.foreach(println)

  }

}