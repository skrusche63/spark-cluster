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

import akka.actor.{Actor,ActorLogging,ActorRef,Props}

import de.kp.spark.cluster.FKMeans
import de.kp.spark.cluster.model._

import de.kp.spark.cluster.source.FeatureSource
import de.kp.spark.cluster.sink.RedisSink

import de.kp.spark.cluster.redis.RedisCache

class FeatureActor(@transient val sc:SparkContext) extends Actor with ActorLogging {

  def receive = {

    case req:ServiceRequest => {
      
      val params = properties(req)

      /* Send response to originator of request */
      sender ! response(req, (params == null))

      if (params != null) {
 
        try {

          RedisCache.addStatus(req,ClusterStatus.STARTED)
          
          val dataset = new FeatureSource(sc).get(req.data)          
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
   
    RedisCache.addStatus(req,ClusterStatus.DATASET)
    
    val (top,iter,strategy) = params   
    /*
     * Determine top k data points that are closest to their
     * respective cluster centroids
     */
    val clustered = new FKMeans().find(dataset,strategy,iter,top).toList
    savePoints(req,new ClusteredPoints(clustered))
    
    /* Update cache */
    RedisCache.addStatus(req,ClusterStatus.FINISHED)
    
  }
  
  private def savePoints(req:ServiceRequest,points:ClusteredPoints) {

    val sink = new RedisSink()
    sink.addPoints(req,points)
     
  }
  
  private def response(req:ServiceRequest,missing:Boolean):ServiceResponse = {
    
    val uid = req.data("uid")
    
    if (missing == true) {
      val data = Map("uid" -> uid, "message" -> Messages.MISSING_PARAMETERS(uid))
      new ServiceResponse(req.service,req.task,data,ClusterStatus.FAILURE)	
  
    } else {
      val data = Map("uid" -> uid, "message" -> Messages.MODEL_BUILDING_STARTED(uid))
      new ServiceResponse(req.service,req.task,data,ClusterStatus.STARTED)	
      
  
    }

  }
  
}