package de.kp.spark.cluster.util
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

import org.apache.spark.rdd.RDD

import org.apache.spark.mllib.clustering.KMeans
import org.apache.spark.mllib.linalg.{Vector,Vectors}

object Optimizer {

  /**
   * Determine from a range of cluster numbers that number where the mean
   * entropy of all cluster labels is minimal; note, that the entropy is 
   * an indicator for the homogenity of the cluster labels
   */ 
  def optimizeByEntropy(data:RDD[(Long,String,Vector)],range:Range,iterations:Int):Int = {
   
    val scores = range.par.map(k => (k, clusterEntropy(data,k,iterations))).toList
    scores.sortBy(_._2).head._1
  
  }
  
  def clusterEntropy(data:RDD[(Long,String,Vector)],clusters:Int,iterations:Int):Double = {

    val vectors = data.map(_._3)
    val model = KMeans.train(vectors,clusters,iterations)

    val entropies = data.map(x => {
      
      val cluster = model.predict(x._3)
      (cluster,x._2)
      
    }).groupBy(_._1).map(data => MathHelper.strEntropy(data._2.map(_._2))).collect()

    entropies.sum / entropies.size
    
  }

  /**
   * Determine from a range of cluster numbers that number where the mean
   * distance between cluster points and their cluster centers is minimal
   */ 
  def optimizeByDistance(data:RDD[(Long,String,Vector)],range:Range,iterations:Int):Int = {

    val scores = range.par.map(k => (k, clusterDistance(data, k, iterations))).toList
    scores.sortBy(_._2).head._1
    
  }

  def distance(a:Array[Double], b:Array[Double]) = 
    Math.sqrt(a.zip(b).map(p => p._1 - p._2).map(d => d * d).sum)

  /**
   * This method calculates the mean distance of all data (vectors) from 
   * their centroids, given certain clustering parameters; the method may
   * be used to score clusters
   */
  def clusterDistance(data: RDD[(Long,String,Vector)], clusters:Int, iterations:Int):Double = {
    
    val vectors = data.map(_._3)
    val model = KMeans.train(vectors,clusters,iterations)
    /**
     * Centroid: Vector that specifies the centre of a certain cluster
     */
    val centroids = model.clusterCenters
  
    val distances = data.map(x => {
      
      val cluster = model.predict(x._3)
      val centroid = centroids(cluster)
      
      distance(centroid.toArray,x._3.toArray)
      
    }).collect()
    
    distances.sum / distances.size

  }

}