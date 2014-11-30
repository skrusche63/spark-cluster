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

import de.kp.spark.cluster.model._
import de.kp.spark.cluster.sink.RedisSink

class ClusterQuestor extends BaseActor {

  implicit val ec = context.dispatcher
  private val sink = new RedisSink()
  
  def receive = {

    case req:ServiceRequest => {
      
      val origin = sender    
      val uid = req.data("uid")

      req.task match {

        case "get:feature" => {
          /*
           * This request retrieves a set of clustered features
            */
          val resp = if (sink.pointsExist(uid) == false) {           
            failure(req,Messages.MODEL_DOES_NOT_EXIST(uid))
            
          } else {    
            
            /* Retrieve path to clustered points for 'uid' from Redis instance */
            val points = sink.points(uid)
            if (points == null) {
              failure(req,Messages.MODEL_DOES_NOT_EXIST(uid))
              
            } else {

              val data = Map("uid" -> uid, "feature" -> points)
              new ServiceResponse(req.service,req.task,data,ClusterStatus.SUCCESS)

            }
          
          }
             
          origin ! resp
          context.stop(self)
          
        }

        case "get:sequence" => {
          /*
           * This request retrieves a set of clustered sequences
            */
          val resp = if (sink.sequencesExist(uid) == false) {           
            failure(req,Messages.MODEL_DOES_NOT_EXIST(uid))
            
          } else {    
            
            /* Retrieve path to clustered sequences for 'uid' from Redis instance */
            val sequences = sink.sequences(uid)
            if (sequences == null) {
              failure(req,Messages.MODEL_DOES_NOT_EXIST(uid))
              
            } else {

              val data = Map("uid" -> uid, "sequence" -> sequences)
              new ServiceResponse(req.service,req.task,data,ClusterStatus.SUCCESS)

            }
          
          }
             
          origin ! resp
          context.stop(self)
          
        }
        
        case _ => {
          
          val msg = Messages.TASK_IS_UNKNOWN(uid,req.task)
          
          origin ! failure(req,msg)
          context.stop(self)
          
        }
      
      }
    
    }
    
    case _ => {
      
      val origin = sender               
      val msg = Messages.REQUEST_IS_UNKNOWN()          
          
      origin ! Serializer.serializeResponse(failure(null,msg))
      context.stop(self)

    }
    
  }
  
}