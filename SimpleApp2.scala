package org.apache.spark.examples.streaming
import scala.collection.mutable.ListBuffer
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
/* SimpleApp.scala */

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

import java.util.Properties
/* SimpleApp.scala */

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

import java.util.Properties

object SimpleApp2 {
  def main(args: Array[String]) {

    val logFile = "hdfs://localhost:9000/input/userdata.txt"; // Should be some file on your system
    val friendsInputFile = "hdfs://localhost:9000/input/soc-LiveJournal1Adj.txt"
    val file = new File("output.txt")
    val bw = new BufferedWriter(new FileWriter(file))
    val conf = new SparkConf().setAppName("Simple Application");
    val sc = new SparkContext(conf);
    val textfile = sc.textFile(friendsInputFile);
    var firstmap = textfile.map(line => line.split("\t"))

    //firstmap.foreach { x => println(x(0).toString + " ... " + x(1).toString) }

    var userA: String = "10"
    var userB: String = "11"

    // Most use of RDD

    var outputmap: Map[String, Int] = Map();
    firstmap = textfile.map(line => line.split("\t")).filter { x =>
      x(0).toString.equals("10") || x(0).toString.equals("11")
    }
    var newmap = firstmap.flatMap { f => f(1).toString.split(",") }
    var mutuals = newmap.map { x => x -> 1 }

    var mutuals1 = mutuals.reduceByKey((a, b) => a + b).filter(x => x._2 == 2)
    var mutualFriends = mutuals1.collect();
    // output to a file
    print(userA+","+userB+" : ");mutualFriends.foreach(x => print(x._1.toString+" "))
}
}