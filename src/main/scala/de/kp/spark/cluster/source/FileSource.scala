package de.kp.spark.cluster.source
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

import de.kp.spark.cluster.Configuration
import de.kp.spark.cluster.model.LabeledPoint

class FileSource(@transient sc:SparkContext) extends Serializable {

  val (fItems,fFeatures) = Configuration.file()

  /**
   * Load labeled features from the file system
   */
  def features(params:Map[String,Any]):RDD[LabeledPoint] = {
    
    sc.textFile(fFeatures).map(valu => {
      
      val Array(label,features) = valu.split(",")  
      new LabeledPoint(label,features.split(" ").map(_.toDouble))
    
    }).cache
    
  }

  /**
   * Load ecommerce items from the file system
   */
  def items(params:Map[String,Any]):RDD[(String,String,String,Long,String,Float)] = {

    sc.textFile(fItems).map(valu => {
      
      val Array(site,user,order,timestamp,item,price) = valu.split(",")  
      (site,user,order,timestamp.toLong,item,price.toFloat)

    
    }).cache
    
  }

}