package de.kp.spark.cluster.rest
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

import akka.actor.ActorSystem
import spray.routing.{Route, SimpleRoutingApp}

object RestService extends SimpleRoutingApp {
  /**
   * A public method to start a web server from a given route.
   */
  def start(route:Route, system:ActorSystem, host:String="127.0.0.1", port:Int =9000) {
    
    implicit val actorSystem = system    
    startServer(host, port)(route)
  
  }

}