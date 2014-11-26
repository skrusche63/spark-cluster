package de.kp.spark.cluster.model
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

import org.json4s._

import org.json4s.native.Serialization
import org.json4s.native.Serialization.{read,write}

import de.kp.spark.core.model._

case class LabeledPoint(
  label:String,features:Array[Double]
)

case class ClusteredPoint(
  cluster:Int,distance:Double,point:LabeledPoint
)

case class ClusteredPoints(items:List[ClusteredPoint])

case class NumberedSequence(sid:Int,data:Array[Array[Int]])

case class ClusteredSequence(
  cluster:Int,similarity:Double,sequence:NumberedSequence
)

case class ClusteredSequences(items:List[ClusteredSequence])

object Serializer extends BaseSerializer {

  def serializeClusteredPoints(points:ClusteredPoints):String = write(points)
  def deserializeClusteredPoints(points:String):ClusteredPoints = read[ClusteredPoints](points)

  def serializeClusteredSequences(sequences:ClusteredSequences):String = write(sequences)
  def deserializeClusteredSequences(sequences:String):ClusteredSequences = read[ClusteredSequences](sequences)
  
}

object Algorithms {
  
  val KMEANS:String = "KMEANS"
  val SKMEANS:String = "SKMEANS"

  private def algorithms = List(KMEANS,SKMEANS)
  
  def isAlgorithm(algorithm:String):Boolean = algorithms.contains(algorithm)
  
}

object Sources {
  /* The names of the data source actually supported */
  val FILE:String    = "FILE"
  val ELASTIC:String = "ELASTIC" 
  val JDBC:String    = "JDBC"    
  val PIWIK:String   = "PIWIK"  
    
  private val sources = List(FILE,ELASTIC,JDBC,PIWIK)
  
  def isSource(source:String):Boolean = sources.contains(source)
  
}

object Messages extends BaseMessages {
 
  def DATA_TO_TRACK_RECEIVED(uid:String):String = String.format("""Data to track received for uid '%s'.""", uid)

  def MODEL_BUILDING_STARTED(uid:String) = String.format("""Model building started for uid '%s'.""", uid)
  
  def MISSING_PARAMETERS(uid:String):String = String.format("""Parameters are missing for uid '%s'.""", uid)

  def MODEL_DOES_NOT_EXIST(uid:String):String = String.format("""The model for uid '%s' does not exist.""", uid)

  def SEARCH_INDEX_CREATED(uid:String):String = String.format("""Search index created for uid '%s'.""", uid)
  
}

object ClusterStatus extends BaseStatus {
  
  val DATASET:String = "dataset"
    
  val STARTED:String = "started"
  val STOPPED:String = "stopped"
    
  val FINISHED:String = "finished"
  val RUNNING:String  = "running"
    
}