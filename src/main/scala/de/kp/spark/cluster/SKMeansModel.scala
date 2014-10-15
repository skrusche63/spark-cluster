package de.kp.spark.cluster
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

/**
 * A clustering model for K-means. Each point belongs to the cluster with the closest center.
 */
class SKMeansModel(val centers: Array[Int], val matrix:SMatrix) extends Serializable {

  /** Total number of clusters. */
  def k: Int = centers.length

  /** Returns the cluster index that a given point belongs to. */
  def predict(point:Int): Int = matrix.findClosest(centers, point)._1

  /**
   * Return the K-means cost for this model on the given data.
   */
  def computeCost(data:RDD[Int]): Double = {

    val bcCenters = data.context.broadcast(centers)
    data.map(point => matrix.pointCost(bcCenters.value, point)).collect().sum
  
  }

}
