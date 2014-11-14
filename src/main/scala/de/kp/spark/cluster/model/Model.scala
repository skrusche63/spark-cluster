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

case class Listener(
  timeout:Int, url:String
)
/**
 * ServiceRequest & ServiceResponse specify the content 
 * sent to and received from the decision service
 */
case class ServiceRequest(
  service:String,task:String,data:Map[String,String]
)
case class ServiceResponse(
  service:String,task:String,data:Map[String,String],status:String
)
/*
 * The Field and Fields classes are used to specify the fields with
 * respect to the data source provided
 */
case class Field(
  name:String,datatype:String,value:String
)
case class Fields(items:List[Field])

/*
 * Service requests are mapped onto job descriptions and are stored
 * in a Redis instance
 */
case class JobDesc(
  service:String,task:String,status:String
)

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

object Serializer {
    
  implicit val formats = Serialization.formats(NoTypeHints)
  
  def serializeFields(fields:Fields):String = write(fields)  
  def deserializeFields(fields:String):Fields = read[Fields](fields)

  def serializeClusteredPoints(points:ClusteredPoints):String = write(points)
  def deserializeClusteredPoints(points:String):ClusteredPoints = read[ClusteredPoints](points)

  def serializeJob(job:JobDesc):String = write(job)
  def deserializeJob(job:String):JobDesc = read[JobDesc](job)

  def serializeResponse(response:ServiceResponse):String = write(response)

  def deserializeRequest(request:String):ServiceRequest = read[ServiceRequest](request)
  def serializeRequest(request:ServiceRequest):String = write(request)

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

object Messages {

  def ALGORITHM_IS_UNKNOWN(uid:String,algorithm:String):String = String.format("""Algorithm '%s' is unknown for uid '%s'.""", algorithm, uid)
 
  def DATA_TO_TRACK_RECEIVED(uid:String):String = String.format("""Data to track received for uid '%s'.""", uid)

  def GENERAL_ERROR(uid:String):String = String.format("""A general error appeared for uid '%s'.""", uid)

  def MODEL_BUILDING_STARTED(uid:String) = String.format("""Model building started for uid '%s'.""", uid)
  
  def MISSING_PARAMETERS(uid:String):String = String.format("""Parameters are missing for uid '%s'.""", uid)

  def MODEL_DOES_NOT_EXIST(uid:String):String = String.format("""The model for uid '%s' does not exist.""", uid)
 
  def NO_ALGORITHM_PROVIDED(uid:String):String = String.format("""No algorithm provided for uid '%s'.""", uid)

  def NO_PARAMETERS_PROVIDED(uid:String):String = String.format("""No parameters provided for uid '%s'.""", uid)

  def NO_SOURCE_PROVIDED(uid:String):String = String.format("""No source provided for uid '%s'.""", uid)

  def REQUEST_IS_UNKNOWN():String = String.format("""Unknown request.""")

  def SEARCH_INDEX_CREATED(uid:String):String = String.format("""Search index created for uid '%s'.""", uid)

  def TASK_ALREADY_STARTED(uid:String):String = String.format("""The task with uid '%s' is already started.""", uid)

  def TASK_DOES_NOT_EXIST(uid:String):String = String.format("""The task with uid '%s' does not exist.""", uid)

  def TASK_IS_UNKNOWN(uid:String,task:String):String = String.format("""The task '%s' is unknown for uid '%s'.""", task, uid)

  def SOURCE_IS_UNKNOWN(uid:String,source:String):String = String.format("""Source '%s' is unknown for uid '%s'.""", source, uid)
  
}

object ClusterStatus {
  
  val DATASET:String = "dataset"
    
  val STARTED:String = "started"
  val STOPPED:String = "stopped"
    
  val FINISHED:String = "finished"
  val RUNNING:String  = "running"
  
  val FAILURE:String = "failure"
  val SUCCESS:String = "success"
    
}