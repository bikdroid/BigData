
package org.apache.spark.examples.streaming
import scala.collection.mutable.ListBuffer
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;

import java.util.Properties
/* SimpleApp.scala */

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

import java.util.Properties

object SimpleApp2 {
  def main(args: Array[String]) {

    val logFile = "hdfs://cshadoop1/bxm142230/input/userdata.txt"; // Should be some file on your system
    val friendsInputFile = "hdfs://cshadoop1/bxm142230/input/soc-LiveJournal1Adj.txt"
    val file = new File("output.txt")
    val bw = new BufferedWriter(new FileWriter(file))
    val conf = new SparkConf().setAppName("Simple Application");
    val sc = new SparkContext(conf);
    val textfile = sc.textFile(friendsInputFile);
    var firstmap = textfile.map(line => line.split("\t"))
    var detailfile = sc.textFile(logFile)
    var namezipmap = detailfile.map{line => var t = line.split(","); var detail = t(1).toString+" "+t(2).toString+":"+t(6).toString; t(0).toString -> detail}
    var userA: String = args(2)
    var userB: String = args(3)

    // Most use of RDD

    // Transformations
    var outputmap: Map[String, Int] = Map();
    firstmap = textfile.map(line => line.split("\t")).filter { x =>
      x(0).toString.equals("11") || x(0).toString.equals("10")
    }
    var newmap = firstmap.flatMap { f => f(1).toString.split(",") }
    var mutuals = newmap.map { x => x -> 1 }

    var mutuals1 = mutuals.reduceByKey((a, b) => a + b).filter(x => x._2 == 2)
 
  var output = mutuals1.join(namezipmap)
    // Actions
    var mutualFriends = output.collect();
    // output to a file
    mutualFriends.foreach(x => bw.write(x._2._2))
    print(userA+","+userB+" : ");print("[");mutualFriends.foreach(x => print(x._2._2+"\t"));print("]");
}