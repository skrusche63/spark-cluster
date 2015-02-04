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

import org.apache.spark._
import org.apache.spark.SparkContext._

import de.kp.spark.core.math.SMatrix
import de.kp.spark.core.model._

import de.kp.spark.cluster.model._
import scala.collection.mutable.ArrayBuffer

class SKMeans extends Serializable {

  /**
   * Default parameters: {k: 2, maxIterations: 20}.
   */
  private var k:Int = 2
  private var iterations:Int = 20
  
  private var matrix:SMatrix = _

  def this(k:Int,iterations:Int,matrix:SMatrix) {
    this()
    
    this.k = k
    this.iterations = iterations
    
    this.matrix = matrix
    
  }
  /** 
   *  Set the number of clusters to create (k). Default: 2.
   */
  def setK(k: Int): this.type = {
    this.k = k
    this
  }

  /** Set maximum number of iterations to run. Default: 20. */
  def setIterations(iterations: Int): this.type = {
    this.iterations = iterations
    this
  }

  /**
   * Train a K-means model on the given set of points; `data` should be cached for high
   * performance, because this is an iterative algorithm.
   */
  private def run(data:RDD[Int]):(Array[Int],SMatrix) = {
    
    val sc = data.sparkContext
    val centers = initRandom(data)

    var iteration = 0
    while (iteration < iterations) {

      val bcCenters = sc.broadcast(centers)
      val bcMatrix  = sc.broadcast(matrix)
      
      val distribution = data.map{point =>
        
        val (pos, sim) = bcMatrix.value.findClosest(bcCenters.value, point)        
        (pos, point)
      
      }.groupBy(_._1)
      
      val newCenters = distribution.map(contrib => {
        
        val pos = contrib._1
        val points = contrib._2.map(_._2).toArray

        val costs = bcMatrix.value.findCosts(points)
        /*
         * Find minimum cost
         */
        val min = costs.min
          
        val newCenter = points(costs.indexOf(min)) 
        newCenter
        
      }).collect()

      (0 until k).foreach(k => centers(k) = newCenters(k))

      println("Centers computed for " + k)
      
      iteration += 1
    
    }

    (centers,matrix)

  }

  /**
   * Initialize `runs` sets of cluster centers at random.
   */
  private def initRandom(data:RDD[Int]):Array[Int] = {
    
    val sample = data.takeSample(true, k, new XORShiftRandom().nextInt())
    sample
    
  }

}

object SKMeans {

  def save(sc:SparkContext, matrix:SMatrix, path:String) {
    
    val output = matrix.serialize()
    sc.parallelize(output,1).saveAsTextFile(path)
    
  }
  
  /**
   * Build similarities from training data
   */
  def prepare(source:RDD[NumberedSequence]):SMatrix = {
    
    val sc = source.sparkContext

    val meas = new SMeasure()   
    val data = source.collect()
    
    val dim = data.length    
    val matrix = new SMatrix(dim,1)

    (0 until dim).foreach(i => {
      ((i+1) until dim).foreach(j => {
        
        val seq_i = data(i)
        val seq_j = data(j)
      
        val row = seq_i.sid
        val col = seq_j.sid
        
        val sim = meas.compute(seq_i.data, seq_j.data)
        matrix.set(row,col,sim)
        
      })
      
    })

    matrix

  }
  
  def detect(data:RDD[NumberedSequence],iterations:Int,k:Int,top:Int,path:String):List[ClusteredSequence] = {
    
    val sc = data.context
    
    /* Restrict to sequence ids */
    val ids = data.map(seq => seq.sid)
    
    /* Retrieve cluster centers and similarity matrix */
    val (centers,matrix) = train(ids,iterations,k,path)
  
    val bccenters = sc.broadcast(centers)
    val bcmatrix  = sc.broadcast(matrix)
    
    val clusters = data.map(sequence => {
      
      val sid = sequence.sid
      val (cluster,sim) = matrix.findClosest(centers, sid)
      
      (cluster,sim,sequence)
      
    })
    /*
     * Retrieve top k sequences (NumberedSequence) with respect to their similarity
     * to the respective cluster center; the cluster identifier is used as a grouping 
     * mechanism to specify which sequences belong to which centroid
     */
    val bctop = sc.broadcast(top)
    clusters.groupBy(_._1).flatMap(group => group._2.toList.sortBy(_._2).reverse.take(bctop.value)).map(data => {
    
      val (cluster,distance,sequence) = data
      new ClusteredSequence(cluster,distance,sequence)
      
    }).collect().toList
    
  }
  
  /**
   * Trains a k-means model using the given set of parameters.
   */
  def train(data:RDD[Int],iterations:Int,k:Int,path:String):(Array[Int],SMatrix) = {
    
    val matrix = SKMeans.load(data.context,path)
    new SKMeans(k,iterations,matrix).run(data)
  
  }
  
  def load(sc:SparkContext, path:String):SMatrix = {
    
    val file = sc.textFile(path).coalesce(1,false)
    val dim = file.count().toInt

    def seqOp(matrix:SMatrix, data:String):SMatrix = {      
      matrix.deserializeRow(data)
      matrix
    }
    /* Note that matrix1 is always NULL */
    def combOp(matrix1:SMatrix,matrix2:SMatrix):SMatrix = matrix2      

    file.aggregate(new SMatrix(dim,1))(seqOp,combOp)    

  }
  
}