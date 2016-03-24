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

object SimpleApp {

  var map2: Map[String, String] = Map()
  def main(args: Array[String]) {
    val logFile = "hdfs://localhost:9000/input/userdata.txt"; // Should be some file on your system
    val friendsInputFile = "hdfs://localhost:9000/input/soc-LiveJournal1Adj.txt"
    val conf = new SparkConf().setAppName("Simple Application");
    val sc = new SparkContext(conf);
    val textfile = sc.textFile(friendsInputFile);
    var firstmap = textfile.map(line => line.split("\t"))

    var findList: List[String] = List("924", "8941", "8942", "9019", "9020", "9021", "9022", "9990", "9992", "9993");

    var firstLevelFriends: Map[String, String] = Map();
    var friendmap: Map[String, List[String]] = Map();
    var suggestionmap: Map[String, List[String]] = Map();
    
    
    // TRANSFORMS

    // get the pairs that have mutual friends
    // seperate pairs that 
    //var map1 = firstmap.map { x => }
    //var map2: Map[String, String] = Map()
    var count = 0
    firstmap.filter(x => x.length == 2).take(40) foreach {
      x =>
        var k = x(0).toString
        //        print(k)
        for (t <- x(1).split(",")) {
          var fkey = k + "," + t.toString
          // print(fkey)
          mapShifter(fkey, "1")

        }
        for (t <- x(1).split(",")) {
          for (u <- x(1).split(",")) {
            // check whether they are equal
            //            print(t.toString+"\t"+u.toString)
            if (!t.equals(u)) {
              var fkey = t.toString + "," + u.toString
              var fkey2 = u.toString + "," + t.toString
              // print(fkey + "and" + fkey2)
              if (map2.contains(fkey)) {
                if (!map2.get(fkey).get.equals("1")) mapShifter(fkey, "0")
              } else {
                mapShifter(fkey, "0")
              }
              if (!map2.contains(fkey2)) {
                mapShifter(fkey2, "0")
              } else {
                if (!map2.get(fkey2).get.equals("1")) mapShifter(fkey2, "0")
              }
            }

          }
        }
        count += 1
    }
    // now all friend-friend map to 1
    // now all friend-suggest map to 0
    var map3 = map2.filter(x => x._2.toString.equals("0"))
    var map3RDD = sc.parallelize(map3.toSeq)

    var newmap = map3RDD.map { x => x._1.toString.split(",")(0) -> x._1.toString.split(",")(1) }
    // newmap.foreach{x => println(x._1.toString+" ---> "+x._2.toString)}

    var finalmap = newmap.groupByKey()
    // finalmap.foreach{x => println(x.toString)} 

    var listmap = sc.parallelize(findList.toSeq)
    var l = listmap.map { x => x -> "1" }
    var outputmap = l.join(finalmap)
    var m = outputmap.map { x =>

      var s = x._2._2.iterator // this is how to get Compact Buffer values
      var t: String = ""
      var c=0
      while (s.hasNext) {t = t+ " " + s.next()} 
      x._1 -> t
    }

    // ACTIONS
    var myoutput = m.collect() //returns CompactBuffer lists for values.

    myoutput.foreach {
      x =>
        println(x._1+" :"+x._2)
    }

  }
  def mapShifter(a: String, b: String) {

    map2 += a -> b;
  }

}

