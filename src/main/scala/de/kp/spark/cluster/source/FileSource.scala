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
import de.kp.spark.cluster.model._

class FileSource(@transient sc:SparkContext) extends Serializable {

  val (feature_in,sequence_in) = Configuration.file()

  /**
   * Load labeled features from the file system
   */
  def features(params:Map[String,Any]):RDD[LabeledPoint] = {
    
    sc.textFile(feature_in).map(valu => {
      
      val Array(label,features) = valu.split(",")  
      new LabeledPoint(label,features.split(" ").map(_.toDouble))
    
    }).cache
    
  }

  /**
   * Read data from file system: it is expected that the lines with
   * the respective text file are already formatted in the SPMF form
   */
  def sequences(params:Map[String,Any] = Map.empty[String,Any]):RDD[NumberedSequence] = {
    
    sc.textFile(sequence_in).filter(line => line.isEmpty == false).map(valu => {
      
      val Array(sid,seq) = valu.split("\\|")  
      val itemsets = seq.replace("-2", "").split(" -1 ").map(v => v.split(" ").map(_.toInt))
    
      new NumberedSequence(sid.toInt,itemsets)

    })
    
  }

}