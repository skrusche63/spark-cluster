package de.kp.spark.cluster.sink
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

import java.util.Date

import de.kp.spark.core.Names

import de.kp.spark.core.model._
import de.kp.spark.core.redis.RedisClient

import de.kp.spark.cluster.Configuration
import de.kp.spark.cluster.model._

import scala.collection.JavaConversions._

class RedisSink {

  val (host,port) = Configuration.redis
  val client = RedisClient(host,port.toInt)

  val service = "cluster"

  def addMatrix(req:ServiceRequest,matrix:String) {
   
    val now = new Date()
    val timestamp = now.getTime()

    val k = "matrix:" + req.data(Names.REQ_SITE) + ":" + req.data(Names.REQ_UID) + ":" + req.data(Names.REQ_NAME) 
    val v = "" + timestamp + ":" + matrix
    
    client.zadd(k,timestamp,v)
    
  }
   
  def matrixExists(req:ServiceRequest):Boolean = {

    val k = "matrix:" + req.data(Names.REQ_SITE) + ":" + req.data(Names.REQ_UID) + ":" + req.data(Names.REQ_NAME) 
    client.exists(k)
    
  }
  
  def matrix(req:ServiceRequest):String = {

    val k = "matrix:" + req.data(Names.REQ_SITE) + ":" + req.data(Names.REQ_UID) + ":" + req.data(Names.REQ_NAME) 
    val matrices = client.zrange(k, 0, -1)

    if (matrices.size() == 0) {
      null
    
    } else {
      
      val last = matrices.toList.last
      last.split(":")(1)
      
    }
  
  }

  def addPoints(req:ServiceRequest,points:ClusteredPoints) {
   
    val now = new Date()
    val timestamp = now.getTime()
    
    val k = "point:" + req.data(Names.REQ_SITE) + ":" + req.data(Names.REQ_UID) + ":" + req.data(Names.REQ_NAME) 
    val v = "" + timestamp + ":" + Serializer.serializeClusteredPoints(points)
    
    client.zadd(k,timestamp,v)
    
  }
   
  def pointsExist(req:ServiceRequest):Boolean = {

    val k = "point:" + req.data(Names.REQ_SITE) + ":" + req.data(Names.REQ_UID) + ":" + req.data(Names.REQ_NAME) 
    client.exists(k)
    
  }

  def points(req:ServiceRequest):String = {

    val k = "point:" + req.data(Names.REQ_SITE) + ":" + req.data(Names.REQ_UID) + ":" + req.data(Names.REQ_NAME) 
    val points = client.zrange(k, 0, -1)

    if (points.size() == 0) {
      null
    
    } else {
      
      val last = points.toList.last
      last.split(":")(1)
      
    }
  
  }

  def addSequences(req:ServiceRequest,sequences:ClusteredSequences) {
   
    val now = new Date()
    val timestamp = now.getTime()
    
    val k = "sequence:" + req.data(Names.REQ_SITE) + ":" + req.data(Names.REQ_UID) + ":" + req.data(Names.REQ_NAME) 
    val v = "" + timestamp + ":" + Serializer.serializeClusteredSequences(sequences)
    
    client.zadd(k,timestamp,v)
    
  }
   
  def sequencesExist(req:ServiceRequest):Boolean = {

    val k = "sequence:" + req.data(Names.REQ_SITE) + ":" + req.data(Names.REQ_UID) + ":" + req.data(Names.REQ_NAME) 
    client.exists(k)
    
  }

  def sequences(req:ServiceRequest):String = {

    val k = "sequence:" + req.data(Names.REQ_SITE) + ":" + req.data(Names.REQ_UID) + ":" + req.data(Names.REQ_NAME) 
    val sequences = client.zrange(k, 0, -1)

    if (sequences.size() == 0) {
      null
    
    } else {
      
      val last = sequences.toList.last
      last.split(":")(1)
      
    }
  
  }

}