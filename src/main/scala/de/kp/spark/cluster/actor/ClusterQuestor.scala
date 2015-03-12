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

import de.kp.spark.core.Names
import de.kp.spark.core.model._

import de.kp.spark.core.redis.RedisDB

import de.kp.spark.cluster.Configuration
import de.kp.spark.cluster.model._

class ClusterQuestor extends BaseActor {

  implicit val ec = context.dispatcher
  val redis = new RedisDB(host,port.toInt)
  
  def receive = {

    case req:ServiceRequest => {
      
      val origin = sender    
      val uid = req.data(Names.REQ_UID)

      val Array(task,topic) = req.task.split(":")
      topic match {

        case "sequence" => {
          /*
           * This request retrieves a set of clustered sequences
            */
          val resp = if (redis.sequencesExist(req) == false) {           
            failure(req,Messages.MODEL_DOES_NOT_EXIST(uid))
            
          } else {    
            
            val sequences = redis.sequences(req)
            if (sequences == null) {
              failure(req,Messages.MODEL_DOES_NOT_EXIST(uid))
              
            } else {

              val data = Map(Names.REQ_UID -> uid, Names.REQ_RESPONSE -> sequences)
              new ServiceResponse(req.service,req.task,data,ClusterStatus.SUCCESS)

            }
          
          }
             
          origin ! resp
          context.stop(self)
          
        }

        case "vector" => {
          /*
           * This request retrieves a set of clustered features
            */
          val resp = if (redis.pointsExist(req) == false) {           
            failure(req,Messages.MODEL_DOES_NOT_EXIST(uid))
            
          } else {    
            
            val points = redis.points(req)
            if (points == null) {
              failure(req,Messages.MODEL_DOES_NOT_EXIST(uid))
              
            } else {

              val data = Map(Names.REQ_UID -> uid, Names.REQ_RESPONSE -> points)
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