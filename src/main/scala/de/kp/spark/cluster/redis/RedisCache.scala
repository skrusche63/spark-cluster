package de.kp.spark.cluster.redis
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

import de.kp.spark.cluster.model._

import java.util.Date
import scala.collection.JavaConversions._

object RedisCache {

  val client  = RedisClient()
  val service = "cluster"
  
  def addMeta(req:ServiceRequest,meta:String) {
   
    val now = new Date()
    val timestamp = now.getTime()
    
    val k = "meta:" + req.data("uid")
    val v = "" + timestamp + ":" + meta
    
    client.zadd(k,timestamp,v)
    
  }
  
  def addStatus(req:ServiceRequest,status:String) {
   
    val (uid,task) = (req.data("uid"),req.task)
   
    val now = new Date()
    val timestamp = now.getTime()
    
    val k = "job:" + service + ":" + uid
    val v = "" + timestamp + ":" + Serializer.serializeJob(JobDesc(service,task,status))
    
    client.zadd(k,timestamp,v)
    
  }
  
  def metaExists(uid:String):Boolean = {

    val k = "meta:" + uid
    client.exists(k)
    
  }
 
  def taskExists(uid:String):Boolean = {

    val k = "job:" + service + ":" + uid
    client.exists(k)
    
  }
  
  /**
   * Get timestamp when job with 'uid' started
   */
  def starttime(uid:String):Long = {
    
    val k = "job:" + service + ":" + uid
    val jobs = client.zrange(k, 0, -1)

    if (jobs.size() == 0) {
      0
    
    } else {
      
      val first = jobs.iterator().next()
      first.split(":")(0).toLong
      
    }
     
  }
  
  def meta(uid:String):String = {

    val k = "meta:" + uid
    val metas = client.zrange(k, 0, -1)

    if (metas.size() == 0) {
      null
    
    } else {
      
      metas.toList.last
      
    }

  }
 
  def status(uid:String):String = {

    val k = "job:" + service + ":" + uid
    val jobs = client.zrange(k, 0, -1)

    if (jobs.size() == 0) {
      null
    
    } else {
      
      val job = Serializer.deserializeJob(jobs.toList.last)
      job.status
      
    }

  }

}