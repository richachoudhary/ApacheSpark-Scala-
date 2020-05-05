package cust_employ_app

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._



    
object SimpleApp {
  def main(args: Array[String]) {
    val logFile = "../README.md" // Should be some file on your system
    val conf = new SparkConf().setMaster("local").setAppName("My App")
    val sc = new SparkContext(conf)
    val logData = sc.textFile(logFile).cache()
    val numAs = logData.filter(line => line.contains("a")).count()
    val numBs = logData.filter(line => line.contains("b")).count()
    println(s"Lines with a: $numAs, Lines with b: $numBs")
    val pythonLines = logData.filter (line => line.contains("Python"))
    println(pythonLines.count())
    println(pythonLines.first())
    pythonLines.take(3).foreach(println)
    sc.stop()
  }
}

