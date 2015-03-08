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
import org.apache.spark.rdd.RDD

import de.kp.spark.core.model._

import de.kp.spark.core.source.SequenceSource
import de.kp.spark.core.source.handler.SequenceHandler

import de.kp.spark.cluster.{RequestContext,SKMeans}
import de.kp.spark.cluster.spec.SequenceSpec

import de.kp.spark.cluster.model._
import de.kp.spark.cluster.sink.RedisSink

import scala.collection.mutable.ArrayBuffer

class SequenceActor(@transient ctx:RequestContext) extends TrainActor(ctx) {
  
  private val base = ctx.config.matrix
  
  override def validate(req:ServiceRequest) = {
    
    if (req.data.contains("k") == false)
      throw new Exception("Parameter 'k' is missing.")
    
    if (req.data.contains("top") == false)
      throw new Exception("Parameter 'top' is missing.")

    if (req.data.contains("iterations") == false)
      throw new Exception("Parameter 'iterations' is missing.")
    
  }
  
  override def train(req:ServiceRequest) {
          
    val source = new SequenceSource(ctx.sc,ctx.config,new SequenceSpec(req))
    val dataset = SequenceHandler.sequence2NumSeq(source.connect(req))

    /*
     * STEP #1: Build similarity matrix
     */
    val matrix = SKMeans.prepare(dataset)

    /* 
     * STEP #2: Save matrix in directory of file system 
     */   
    val now = new Date()
    val dir = base + "/skmeans-" + now.getTime().toString
    
    SKMeans.save(ctx.sc,matrix,dir)
    
    /* Put directory to RedisSink for later requests */
    new RedisSink().addMatrix(req,dir)

    /*
     * STEP #3: Detect top similiar sequences with respect
     * to their cluster centroids
     */         
    val params = ArrayBuffer.empty[Param]
    
    val k = req.data("k").asInstanceOf[Int]
    params += Param("k","integer",k.toString)

    val top = req.data("top").asInstanceOf[Int]
    params += Param("top","integer",top.toString)
      
    val iterations = req.data("iterations").asInstanceOf[Int]
    params += Param("iterations","integer",iterations.toString)

    cache.addParams(req, params.toList)
    
    val clustered = SKMeans.detect(dataset,iterations,k,top,dir)
    saveSequences(req,new ClusteredSequences(clustered))
   
  }
  
  private def saveSequences(req:ServiceRequest,sequences:ClusteredSequences) {
    
    val sink = new RedisSink()
    sink.addSequences(req, sequences)
    
  }

}