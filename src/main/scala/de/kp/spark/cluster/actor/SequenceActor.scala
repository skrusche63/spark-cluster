package de.kp.spark.cluster.actor
/* Copyright (c) 2014 Dr. Krusche & Partner PartG
* 
* This file is part of the Spark-Cluster project
* (https://github.com/skrusche63/spark-outlier).
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

import java.util.Date

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import de.kp.spark.cluster.{Configuration,SKMeans}
import de.kp.spark.cluster.model._

import de.kp.spark.cluster.source.SequenceSource
import de.kp.spark.cluster.sink.RedisSink

import de.kp.spark.cluster.redis.RedisCache

class SequenceActor(@transient val sc:SparkContext) extends BaseActor {
  
  private val base = Configuration.matrix

  def receive = {

    case req:ServiceRequest => {
      
      val params = properties(req)

      /* Send response to originator of request */
      sender ! response(req, (params == null))

      if (params != null) {
 
        try {

          RedisCache.addStatus(req,ClusterStatus.STARTED)
          
          val dataset = new SequenceSource(sc).get(req.data)          
          findClusters(req,dataset,params)

        } catch {
          case e:Exception => RedisCache.addStatus(req,ClusterStatus.FAILURE)          
        }

      }
      
      context.stop(self)
          
    }
    
    case _ => {
      
      log.error("unknown request.")
      context.stop(self)
      
    }
    
  }
  
  private def properties(req:ServiceRequest):(Int,Int,Int) = {
      
    try {
      
      val k = req.data("k").asInstanceOf[Int]
      val top = req.data("top").asInstanceOf[Int]
      
      val iter = req.data("interations").asInstanceOf[Int]
        
      return (top,k,iter)
        
    } catch {
      case e:Exception => {
         return null          
      }
    }
    
  }
  
  private def findClusters(req:ServiceRequest,dataset:RDD[NumberedSequence],params:(Int,Int,Int)) {

    RedisCache.addStatus(req,ClusterStatus.DATASET)

    /*
     * STEP #1: Build similarity matrix
     */
    val matrix = SKMeans.prepare(dataset)

    /* 
     * STEP #2: Save matrix in directory of file system 
     */   
    val now = new Date()
    val dir = base + "/skmeans-" + now.getTime().toString
    
    SKMeans.save(sc,matrix,dir)
    
    /* Put directory to RedisSink for later requests */
    new RedisSink().addMatrix(req,dir)

    /*
     * STEP #3: Detect top similiar sequences with respect
     * to their cluster centroids
     */    
    
    val (top,k,iterations) = params 
    
    val clustered = SKMeans.detect(dataset,iterations,k,top,dir)
    saveSequences(req,new ClusteredSequences(clustered))
          
    /* Update cache */
    RedisCache.addStatus(req,ClusterStatus.FINISHED)
    
    /* Notify potential listeners */
    notify(req,ClusterStatus.FINISHED)
    
  }
  
  private def saveSequences(req:ServiceRequest,sequences:ClusteredSequences) {
    
    val sink = new RedisSink()
    sink.addSequences(req, sequences)
    
  }

}