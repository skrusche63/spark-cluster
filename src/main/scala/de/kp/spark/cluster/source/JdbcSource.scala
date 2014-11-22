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

import de.kp.spark.cluster.model._

import de.kp.spark.cluster.io.JdbcReader

class JdbcSource(@transient sc:SparkContext) extends Serializable {

  def connect(params:Map[String,Any],fields:List[String]):RDD[Map[String,Any]] = {
    
    val site = params("site").asInstanceOf[Int]
    val query = params("query").asInstanceOf[String]
    
    new JdbcReader(sc,site,query).read(fields)
    
  }

}