package de.kp.spark.cluster.sample

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import de.kp.spark.cluster.model._
import de.kp.spark.cluster.SKMeans

object S2MPApp extends SparkApp {

  def main(args:Array[String]) {
    
    val input = "/Work/tmp/spmf/BMS1_spmf"    
    val path  = "/Work/tmp/fsm/matrix"
 
    val sc = createLocalCtx("TSRApp")

    var start = System.currentTimeMillis()
    
    val data = getItemsets(sc,input).cache()

    val mid1 = System.currentTimeMillis()
    println("==================================")
    println("Load time: " + (mid1-start) + " ms")

    prepareAndSave(data,path)

    val points = data.map(seq => seq.sid)
    buildModel(points, 2, 2, path)

    val end = System.currentTimeMillis()
    println("==================================")
    println("Total time: " + (end-start) + " ms")

    
  }
  
  private def getItemsets(sc:SparkContext, input:String):RDD[NumberedSequence] = {
    
    val file = sc.textFile(input)
    file.map(line => {
      
      val Array(sid,sequence) = line.split("\\|")
      val itemsets = sequence.replace("-2", "").split(" -1 ").map(v => v.split(" ").map(_.toInt))
      
      new NumberedSequence(sid.toInt,itemsets)
      
    })
    
  }
  
  private def buildModel(data:RDD[Int], k:Int, iterations:Int, path:String) {
    val start = System.currentTimeMillis()
 
    val model = SKMeans.train(data,k,iterations,path)
    
    val end = System.currentTimeMillis()
    println("==================================")
    println("Build model time: " + (end-start) + " ms")
    
  }
  
  private def prepareAndSave(source:RDD[NumberedSequence],path:String) = {

    val start = System.currentTimeMillis()
 
    val matrix = SKMeans.prepare(source)
    SKMeans.save(source.context,matrix,path)
    
    val end = System.currentTimeMillis()
    println("==================================")
    println("Prepare matrix time: " + (end-start) + " ms")
    
  }

}