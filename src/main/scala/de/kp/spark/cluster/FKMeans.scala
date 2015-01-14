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

import org.apache.spark.mllib.clustering.KMeans
import org.apache.spark.mllib.linalg.{Vector,Vectors}

import de.kp.spark.cluster.model._
import de.kp.spark.cluster.util.{MathHelper,Optimizer}

class FKMeans extends Serializable {

  /**
   * This method specifies the interface for the EXPLICIT clustering approach
   */
  def find(data:RDD[LabeledPoint],k:Int,iterations:Int):(Array[Vector], RDD[(Int,Long,Double)]) = {

    /*
     * STEP #1: Z-score normalization of training data and
     * training of KMeans  model from these normalized data
     */
    val normdata = normalize(data)

    val vectors = normdata.map(_._3)
    val model = KMeans.train(vectors,k,iterations)
    /*
     * STEP #2: Determine centroids, i.e. feature description
     * of the cluster centers, and assign cluster center to
     * the training data
     */
    val centroids = model.clusterCenters
    val clustered = normdata.map(x => {

      val (row,label,vector) = x
      
      val cluster = model.predict(vector)
      val centroid = centroids(cluster)
      
      val distance = Optimizer.distance(centroid.toArray,vector.toArray)
      
      (cluster,row,distance)
    
    })
    
    (centroids,clustered)
  
  }
  
  /**
   * This method specifies the interface for the IMPLICIT clustering approach
   */
  def find(data:RDD[LabeledPoint],top:Int,iterations:Int,strategy:String="entropy"):List[ClusteredPoint] = {
    
    val (k,normdata) = prepare(data,strategy,iterations)
    detect(normdata,k,iterations,top)
    
  }
  
  def detect(normdata:RDD[(Long,String,Vector)],k:Int,iterations:Int,top:Int):List[ClusteredPoint] = {
    
    val sc = normdata.context
    /*
     * STEP #1: Compute KMeans model
     */   
    val vectors = normdata.map(_._3)

    val model = KMeans.train(vectors,k,iterations)
    val centroids = model.clusterCenters

    /*
     * STEP #2: Calculate the distances for all points from their clusters; 
     * outliers are those that have the farest distance
     */
    val clusters = normdata.map(x => {
      
      val cluster = model.predict(x._3)
      val centroid = centroids(cluster)
      
      val distance = Optimizer.distance(centroid.toArray,x._3.toArray)
     
      (cluster,distance,x)
      
    })

    /*
     * Retrieve top k features (LabeledPoint) with respect to their clusters;
     * the cluster identifier is used as a grouping mechanism to specify which
     * features belong to which centroid
     */
    val bctop = sc.broadcast(top)
    clusters.groupBy(_._1).flatMap(group => group._2.toList.sortBy(_._2).take(bctop.value)).map(data => {
    
      val (cluster,distance,(row,label,vector)) = data
      new ClusteredPoint(cluster,distance,LabeledPoint(row,label,vector.toArray))
      
    }).collect().toList

  }

  def prepare(data:RDD[LabeledPoint],strategy:String="entropy",iterations:Int):(Int,RDD[(Long,String,Vector)]) = {
    
    /* 
     * STEP #1: Normalize data 
     */
    val normdata = normalize(data)
    
    /*
     * STEP #2: Find optimal number of clusters
     */
  
    /* Range of cluster center */
    val range = (5 to 40 by 5)
  
    val k = strategy match {
      
      case "distance" => Optimizer.optimizeByDistance(normdata, range, iterations)
        
      case "entropy"  => Optimizer.optimizeByEntropy(normdata, range, iterations)
      
    }
    
    (k, normdata)
  
  }
  
  private def normalize(data:RDD[LabeledPoint]):RDD[(Long,String,Vector)] = {
    
    val ds1 = data.map(x => (x.id,x.label))
    val ds2 = data.map(p => p.features)
    
    val normalized = MathHelper.normalize(ds2)    
    ds1.zip(normalized).map(v => {
      
      val (row,label) = v._1
      val vector = Vectors.dense(v._2)
      
      (row,label,vector)
      
    }) 
  }
  
}