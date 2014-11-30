package de.kp.spark.cluster.actor
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

import de.kp.spark.core.actor.BaseIndexer
import de.kp.spark.core.model._

import de.kp.spark.cluster.Configuration

class ClusterIndexer extends BaseIndexer(Configuration) {
 
  override def getSpec(req:ServiceRequest):(List[String],List[String]) = {
    
    req.task.split(":")(1) match {
      
      case "feature" => {
    
        val names = req.data("names").split(",").toList
        val types = req.data("types").split(",").toList
    
        (names,types)
        
      }
      case "sequence" => {
        
        (List.empty[String],List.empty[String])
        
      }
      
      case _ => throw new Exception("Unknown topic.")
    }
    
  }  
 
  override def getTopic(req:ServiceRequest):String = {
    
    req.task.split(":")(1) match {
      
      case "feature" => "feature"
      case "sequence" => "item"
      
      case _ => throw new Exception("Unknown topic.")
    }
    
  }
}