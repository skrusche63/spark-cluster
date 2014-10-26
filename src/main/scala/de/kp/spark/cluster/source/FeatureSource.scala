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

import de.kp.spark.cluster.model._

/**
 * FeatureSource is an abstraction layer on top the physical 
 * data sources supported by KMeans outlier detection
 */
class FeatureSource(@transient sc:SparkContext) {

  def get(data:Map[String,String]):RDD[LabeledPoint] = {

    val source = data("source")
    source match {
      
      /* 
       * Discover clusters from feature set persisted as an appropriate search 
       * index from Elasticsearch; the configuration parameters are retrieved 
       * from the service configuration 
       */    
      case Sources.ELASTIC => new ElasticSource(sc).features(data)
      /* 
       * Discover clusters from feature set persisted as a file on the (HDFS) 
       * file system; the configuration parameters are retrieved from the service 
       * configuration  
       */    
      case Sources.FILE => new FileSource(sc).features(data)
      /*
       * Discover clusters from feature set persisted as an appropriate table 
       * from a JDBC database; the configuration parameters are retrieved from 
       * the service configuration
       */
      case Sources.JDBC => new JdbcSource(sc).features(data)
      
      case _ => null
      
    }

  }
}