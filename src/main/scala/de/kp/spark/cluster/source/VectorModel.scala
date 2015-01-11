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

      val label = data(spec.value(Names.LBL_FIELD))
      val value = data(spec.value(Names.VAL_FIELD))
      
      (row,col,label,value)
      
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

      val label = data(spec.value(Names.LBL_FIELD)).asInstanceOf[String] 
      val value = data(spec.value(Names.VAL_FIELD)).asInstanceOf[String] 
      
      (row,col,label,value)
      
    })

    buildLabeledPoints(dataset)
    
  }
  
  def buildJDBC(req:ServiceRequest,rawset:RDD[Map[String,Any]]):RDD[LabeledPoint] = {
    
    val spec = sc.broadcast(Features.get(req))
    val dataset = rawset.map(data => {
      
      val row = data(spec.value(Names.ROW_FIELD)).asInstanceOf[Long]
      val col = data(spec.value(Names.COL_FIELD)).asInstanceOf[Long]

      val label = data(spec.value(Names.LBL_FIELD)).asInstanceOf[String] 
      val value = data(spec.value(Names.VAL_FIELD)).asInstanceOf[String] 
      
      (row,col,label,value)
       
    })

    buildLabeledPoints(dataset)
    
  }

  /**
   * This method creates a set of labeled datapoints that are 
   * used for clustering or similarity analysis
   */
  private def buildLabeledPoints(dataset:RDD[(Long,Long,String,String)]):RDD[LabeledPoint] = {
  
    /*
     * The dataset specifies a 'sparse' data description;
     * in order to generate dense vectors from it, we first
     * have to determine the minimum (= 0) and maximum column
     * value to create equal size vectors
     */
    val size = sc.broadcast((dataset.map(_._2).max + 1).toInt)
    
    dataset.groupBy(x => x._1).map(x => {
      
      /*
       * The label is a denormalized value and is assigned to
       * each column specific dataset as well; this implies
       * that we only need this value once
       */
      val label = x._2.head._3
      val features = Array.fill[Double](size.value)(0)
      
      val data = x._2.map(v => (v._2.toInt,v._4.toDouble)).toSeq.sortBy(v => v._1)
      data.foreach(x => features(x._1) = x._2)
      
      new LabeledPoint(label,features)
      
    })
 
  }

}