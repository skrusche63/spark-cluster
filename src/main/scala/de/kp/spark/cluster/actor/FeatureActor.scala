package de.kp.spark.cluster.actor
/* Copyright (c) 2014 Dr. Krusche & Partner PartG
* 
* This file is part of the Spark-Cluster project
* (https://github.com/skrusche63/spark-cluster).
* 
* Spark-Cluster is free software: you can redistribute it and/or modify it under the
* terms of the GNU General Public License as published by the Free Software
* Foundation, either version 3 of the License, or (at your option) any later
* version.
* 
* Spark-Cluster is distributed in the hope that it will be useful, but WITHOUT ANY
* WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR
* A PARTICULAR PURPOSE. See the GNU General Public License for more details.
* You should have received a copy of the GNU General Public License along with
* Spark-Cluster. 
* 
* If not, see <http://www.gnu.org/licenses/>.
*/

import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD

import org.apache.spark.mllib.linalg.Vector

import de.kp.spark.core.Names
import de.kp.spark.core.model._

import de.kp.spark.core.source.VectorSource
import de.kp.spark.core.source.handler.VectorHandler

import de.kp.spark.cluster.{FKMeans,RequestContext}
import de.kp.spark.cluster.model._

import de.kp.spark.cluster.sink.RedisSink
import de.kp.spark.cluster.spec.FeatureSpec

import scala.collection.mutable.ArrayBuffer

class FeatureActor(@transient ctx:RequestContext) extends TrainActor(ctx) {
  
  import ctx.sqlc.createSchemaRDD
  
  override def validate(req:ServiceRequest) {
      
    /*
     * The Similarity Analysis engine supports two different approaches
     * to clustering: a) explicit clustering, and b) implicit clustering
     * 
     * Explicit clustering means, that the number of clusters is explicitly
     * defined by the request; in this case the number of clusters, 'k', and
     * the number of iterations ,'iterations', must be provided.
     * 
     * Implicit clustering means, that the number of clusters are determined
     * by an internal optimization algorithm; in this case the optimization
     * strategy, 'strategy', must be provided, also the number, 'top', points
     * that are nearest to their cluster centroids.
     * 
     * The requested approach must not be specified externally, but is inferred
     * from the combination of the input parameters
     */
    if (req.data.contains("strategy") == true && req.data("strategy") != "") {
      /*
       * Requests that specify an optimization strategy are considered
       * as IMPLICIT clustering requests. In this case, we additionally
       * expect a parameter 'top' and 'iterations'
       */
      if (req.data.contains("top") == false)
        throw new Exception("Parameter 'top' is missing.")
        
      if (req.data.contains("iterations") == false)
        throw new Exception("Parameter 'iterations' is missing.")
      
    } else {
      /*
       * Requests that specify no optimization strategy are considered
       * as EXPLICIT clustering requests. Here, we expect a parameter 
       * 'k' to specify the number of clusters, and 'iterations'
       */
      if (req.data.contains("k") == false) 
        throw new Exception("Parameter 'k' is missing.")
        
      if (req.data.contains("iterations") == false)
        throw new Exception("Parameter 'iterations' is missing.")
          
    }
    
  }
  
  override def train(req:ServiceRequest) {
          
    val source = new VectorSource(ctx.sc,ctx.config,new FeatureSpec(req))
    val dataset = VectorHandler.vector2LabeledPoints(source.connect(req))

    /*
     * We distinguish between explicit and implicit clustering
     */
    if (req.data.contains("strategy") == true && req.data("strategy") != "") {
      
      /********** IMPLICIT **********/
      
      val params = ArrayBuffer.empty[Param]
      
      val strategy = req.data("strategy")
      params += Param("strategy","string",strategy)

      val top = req.data("top").toInt
      params += Param("top","integer",top.toString)

      val iter = req.data("iterations").toInt
      params += Param("iterations","integer",iter.toString)

      cache.addParams(req, params.toList)
      
      /*
       * Determine top k data points that are closest to their
       * respective cluster centroids
       */
      val clustered = new FKMeans().find(dataset,top,iter,strategy).toList
      savePoints(req,new ClusteredPoints(clustered))
    
    } else {
      
      /********** EXPLICIT **********/
      
      val params = ArrayBuffer.empty[Param]
      
      val k = req.data("k").toInt
      params += Param("k","integer",k.toString)

      val iter = req.data("iterations").toInt
      params += Param("iterations","integer",iter.toString)

      cache.addParams(req, params.toList)
      /*
       * Determine the controids and assign the respective cluster
       * centers to the dataset provided; the result of the FKMeans
       * mechanism is an RDD with a (cluster,row) assignment
       */
      val (centroids,clustered) = new FKMeans().find(dataset,k,iter)
      /*
       * Save centroids and clustered dataset as Parquet files
       */
      saveCentroids(req,centroids)
      saveClustered(req,clustered)
      
    }
    
  }
  
  private def saveCentroids(req:ServiceRequest,centroids:Array[Vector]) {
    
    /* url = e.g.../part1/part2/part3/1 */
    val url = req.data(Names.REQ_URL)
   
    val pos = url.lastIndexOf('/')
    
    val base = url.substring(0, pos)
    val step = url.substring(pos+1).toInt + 1
    
    val store = base + "/" + (step + 1)
    val table = ctx.sc.parallelize(centroids.zipWithIndex.map(x => {
      
      val (vector,cluster) = x
      ParquetCentroid(cluster,vector.toArray.toSeq)
        
    }))
    
    table.saveAsParquetFile(store)  
  
  }
  
  private def saveClustered(req:ServiceRequest,dataset:RDD[(Int,Long,Double)]) {
    
    /* url = e.g.../part1/part2/part3/1 */
    val url = req.data(Names.REQ_URL)
   
    val pos = url.lastIndexOf('/')
    
    val base = url.substring(0, pos)
    val step = url.substring(pos+1).toInt + 2
    
    val store = base + "/" + (step + 1)
    val table = dataset.map(x => ParquetClustered(x._1,x._2,x._3))
    
    table.saveAsParquetFile(store)  

  }
  
  private def savePoints(req:ServiceRequest,points:ClusteredPoints) {

    val sink = new RedisSink()
    sink.addPoints(req,points)
     
  }
   
}