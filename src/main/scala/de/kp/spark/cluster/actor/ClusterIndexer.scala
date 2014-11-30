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

import de.kp.spark.core.model._
import de.kp.spark.core.io.ElasticIndexer

import de.kp.spark.cluster.model._
import de.kp.spark.cluster.io.{ElasticBuilderFactory => EBF}

class ClusterIndexer extends BaseActor {
  
  def receive = {
    
    case req:ServiceRequest => {
     
      val uid = req.data("uid")
      val origin = sender    

      val index   = req.data("index")
      val mapping = req.data("type")

      req.task match {

        case "index:feature" => {

          try {
    
            val (names,types) = fieldspec(req.data)
    
            val builder = EBF.getBuilder("feature",mapping,names,types)
            val indexer = new ElasticIndexer()
    
            indexer.create(index,mapping,builder)
            indexer.close()
      
            val data = Map("uid" -> uid, "message" -> Messages.SEARCH_INDEX_CREATED(uid))
            val response = new ServiceResponse(req.service,req.task,data,ClusterStatus.SUCCESS)	
      
            val origin = sender
            origin ! response
      
          } catch {
        
            case e:Exception => {
          
              log.error(e, e.getMessage())
      
              val data = Map("uid" -> uid, "message" -> e.getMessage())
              val response = new ServiceResponse(req.service,req.task,data,ClusterStatus.FAILURE)	
      
              val origin = sender
              origin ! response
          
            }
      
          } finally {        
            context.stop(self)

          }

        }
        
        case "index:sequence" => {

          try {
  
            val builder = EBF.getBuilder("item",mapping)
            val indexer = new ElasticIndexer()
    
            indexer.create(index,mapping,builder)
            indexer.close()
      
            val data = Map("uid" -> uid, "message" -> Messages.SEARCH_INDEX_CREATED(uid))
            val response = new ServiceResponse(req.service,req.task,data,ClusterStatus.SUCCESS)	
      
            val origin = sender
            origin ! response
      
          } catch {
        
            case e:Exception => {
          
              log.error(e, e.getMessage())
      
              val data = Map("uid" -> uid, "message" -> e.getMessage())
              val response = new ServiceResponse(req.service,req.task,data,ClusterStatus.FAILURE)	
      
              val origin = sender
              origin ! response
          
            }
      
          } finally {        
            context.stop(self)

          }
          
        }
        
        case _ => {
          
          val msg = Messages.TASK_IS_UNKNOWN(uid,req.task)
          
          origin ! failure(req,msg)
          context.stop(self)
          
        }
        
      }

    }
    
  }
 
  private def fieldspec(params:Map[String,String]):(List[String],List[String]) = {

    val names = params("names").split(",").toList
    val types = params("types").split(",").toList
    
    (names,types)
    
  }  
 
}