package org.apache.spark.examples.streaming

/* SimpleApp.scala */
import scala.collection.mutable.ListBuffer
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

import java.util.Properties
import java.util.Calendar;

object SimpleApp2 {

  var friendsMap: Map[String, List[String]] = Map()

  def main(args: Array[String]) {

    val logFile = "hdfs://localhost:9000/input/userdata.txt"; // Should be some file on your system
    val friendsInputFile = "hdfs://localhost:9000/input/soc-LiveJournal1Adj.txt"

    val file = new File("output.txt")
    val bw = new BufferedWriter(new FileWriter(file))
    val conf = new SparkConf().setAppName("Simple Application");
    val sc = new SparkContext(conf);
    val textfile = sc.textFile(friendsInputFile);
    val usertext = sc.textFile(logFile);
    var temp: Map[String, Int] = Map()

    var sortedAges = scala.collection.immutable.Map[String, Double]()
    // Most use of RDD

    // Transformations

    var useragemap = usertext.map { x =>
      var t = x.toString.split(",");
      var dob = 2016 - t(9).toString.split("/")(2).toInt;

      t(0) -> dob
    }.toArray.toMap

    var usernamemap = usertext.map { x =>
      var t = x.toString.split(",");
      var name = t(1).toString+" "+t(2).toString

      t(0) -> name
    }
    
    var useraddrmap = usertext.map { x =>
      var t = x.split(",")
      var addr = t(3) + "," + t(4) + "," + t(5) + "," + t(7)
      t(0) -> addr
    }
    var firstmap = textfile.map { x => x.split("\t") }

    var auxmap = firstmap.toArray()
    // var finalmap = scala.collection.mutable.Map[String, (Float, String)]()
    // we will collect all the 

    auxmap.map { x =>
      if (x.length > 1) {

        //fillFriendsMap(x(0).toString, x(1).toString.split(","))

        var list: List[String] = List()
        list = x(1).toString.split(",").toList
        friendsMap += x(0).toString -> list

      } else {

        //fillFriendsMap(x(0), Array())
        var list: List[String] = List()
        list = List()
        friendsMap += x(0) -> list

      }
    }

    // logic for getting average

    var averagemap = friendsMap.map { i => (i._1, (friendsMap.get(i._1).map { x => x.map { y => (useragemap.get(y).get) } }).toList.flatten) }
    var agemap: Map[String, Double] = Map()
    averagemap.map {
      x =>
        if (x._2.toList.length.toFloat == 0) agemap += x._1 -> 0
        else {
          var sum: Double = 0.0;

          x._2.toList.foreach(sum += _)
          agemap += x._1 -> sum / x._2.length
        }
    }

    /// Sorting by the average age.
    sortedAges = agemap.toList.sortBy(f => f._2).reverse.take(20).toMap

    /// Converting to a RDD map.

    // all transformations done
    var sortedAgesRDD = sc.parallelize(sortedAges.toSeq)
    var map1 = sortedAgesRDD.map(f => f._1.toString -> f._2)
    var finalmap = map1.join(useraddrmap)
    var finalmap2 = usernamemap.join(finalmap)
    //finalmap2.take(10).foreach{x => println(x.toString)}

    // Actions
    var m = finalmap2.collect().toList
    m.foreach{x => println(x._2._1+" ,"+x._2._2._2+" ,"+x._2._2._1)}
    // output to a file
    
  }

}