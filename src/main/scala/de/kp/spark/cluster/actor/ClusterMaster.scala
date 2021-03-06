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

import akka.actor.{ActorRef,Props}

import de.kp.spark.core.actor._
import de.kp.spark.core.model._

import de.kp.spark.cluster.RequestContext

class ClusterMaster(@transient ctx:RequestContext) extends BaseMaster(ctx.config) {
  
  override def actor(worker:String):ActorRef = {
    
    worker match {
     /*
       * Metadata management is part of the core functionality; field or metadata
       * specifications can be registered in, and retrieved from a Redis database.
       */
      case "fields"   => context.actorOf(Props(new FieldQuestor(ctx.config)))
      case "register" => context.actorOf(Props(new BaseRegistrar(ctx.config)))        
      /*
       * Index management is part of the core functionality; an Elasticsearch 
       * index can be created and appropriate (tracked) items can be saved.
       */  
      case "index" => context.actorOf(Props(new BaseIndexer(ctx.config)))
      case "track" => context.actorOf(Props(new BaseTracker(ctx.config)))

      case "params" => context.actorOf(Props(new ParamQuestor(ctx.config)))
      /*
       * Request the actual status of an association rule mining 
       * task; note, that get requests should only be invoked after 
       * having retrieved a FINISHED status.
       * 
       * Status management is part of the core functionality.
       */
      case "status" => context.actorOf(Props(new StatusQuestor(ctx.config)))

      case "get"   => context.actorOf(Props(new ClusterQuestor()))
      case "train" => context.actorOf(Props(new ClusterBuilder(ctx)))       
     
      case _ => null
      
    }
  
  }

}