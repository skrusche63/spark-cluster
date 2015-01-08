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

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import de.kp.spark.core.model._

import de.kp.spark.cluster.FKMeans
import de.kp.spark.cluster.model._

import de.kp.spark.cluster.source.VectorSource
import de.kp.spark.cluster.sink.RedisSink

class FeatureActor(@transient val sc:SparkContext) extends BaseActor {

  def receive = {

    case req:ServiceRequest => {
      
      val params = properties(req)

      /* Send response to originator of request */
      sender ! response(req, (params == null))

      if (params != null) {
 
        try {

          cache.addStatus(req,ClusterStatus.STARTED)
          
          val dataset = new VectorSource(sc).get(req)          
          findClusters(req,dataset,params)

        } catch {
          case e:Exception => cache.addStatus(req,ClusterStatus.FAILURE)          
        }

      }
      
      context.stop(self)
          
    }
    
    case _ => {
      
      log.error("unknown request.")
      context.stop(self)
      
    }
    
  }
  
  private def properties(req:ServiceRequest):(Int,Int,String) = {
      
    try {
      
      val top = req.data("top").asInstanceOf[Int]
      val iter = req.data("iterations").asInstanceOf[Int]
      
      val strategy = req.data("strategy").asInstanceOf[String]
        
      return (top,iter,strategy)
        
    } catch {
      case e:Exception => {
         return null          
      }
    }
    
  }
  
  private def findClusters(req:ServiceRequest,dataset:RDD[LabeledPoint],params:(Int,Int,String)) {
   
    cache.addStatus(req,ClusterStatus.DATASET)
    
    val (top,iter,strategy) = params   
    /*
     * Determine top k data points that are closest to their
     * respective cluster centroids
     */
    val clustered = new FKMeans().find(dataset,strategy,iter,top).toList
    savePoints(req,new ClusteredPoints(clustered))
    
    /* Update cache */
    cache.addStatus(req,ClusterStatus.TRAINING_FINISHED)
    
    /* Notify potential listeners */
    notify(req,ClusterStatus.TRAINING_FINISHED)
    
  }
  
  private def savePoints(req:ServiceRequest,points:ClusteredPoints) {

    val sink = new RedisSink()
    sink.addPoints(req,points)
     
  }
   
}