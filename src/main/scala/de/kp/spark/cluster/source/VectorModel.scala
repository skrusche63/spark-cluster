package de.kp.spark.cluster.source
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

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import de.kp.spark.core.Names
import de.kp.spark.core.model._

import de.kp.spark.cluster.model._
import de.kp.spark.cluster.spec.Features

import scala.collection.mutable.ArrayBuffer

class VectorModel(@transient sc:SparkContext) extends Serializable {
  
  def buildElastic(req:ServiceRequest,rawset:RDD[Map[String,String]]):RDD[LabeledPoint] = {
   
    val spec = sc.broadcast(Features.get(req))
    val dataset = rawset.map(data => {
      
      val row = data(spec.value(Names.ROW_FIELD)).toLong
      val col = data(spec.value(Names.COL_FIELD)).toLong

      val value = data(spec.value(Names.VAL_FIELD))
      (row,col,value)
      
    })

    buildLabeledPoints(dataset)
    
  }
  
  def buildFile(req:ServiceRequest,rawset:RDD[String]):RDD[LabeledPoint] = {
    
    rawset.map(valu => {
      
      val Array(label,features) = valu.split(",")  
      new LabeledPoint(label,features.split(" ").map(_.toDouble))
    
    })
    
  }
  
  def buildParquet(req:ServiceRequest,rawset:RDD[Map[String,Any]]):RDD[LabeledPoint] = {
    
    val spec = sc.broadcast(Features.get(req))
    val dataset = rawset.map(data => {
      
      val row = data(spec.value(Names.ROW_FIELD)).asInstanceOf[Long]
      val col = data(spec.value(Names.COL_FIELD)).asInstanceOf[Long]

      val value = data(spec.value(Names.VAL_FIELD)).asInstanceOf[String] 
      (row,col,value)
      
    })

    buildLabeledPoints(dataset)
    
  }
  
  def buildJDBC(req:ServiceRequest,rawset:RDD[Map[String,Any]]):RDD[LabeledPoint] = {
    
    val spec = sc.broadcast(Features.get(req))
    val dataset = rawset.map(data => {
      
      val row = data(spec.value(Names.ROW_FIELD)).asInstanceOf[Long]
      val col = data(spec.value(Names.COL_FIELD)).asInstanceOf[Long]

      val value = data(spec.value(Names.VAL_FIELD)).asInstanceOf[String] 
      (row,col,value)
      
    })

    buildLabeledPoints(dataset)
    
  }

  private def buildLabeledPoints(dataset:RDD[(Long,Long,String)]):RDD[LabeledPoint] = {
  
    dataset.groupBy(x => x._1).map(x => {
      
      val data = x._2.map(v => (v._2,v._3)).toSeq.sortBy(v => v._1)
      
      val label = data.head._2
      val features = data.tail.map(_._2.toDouble)
      
      new LabeledPoint(label,features.toArray)
    
    })
 
  }

}