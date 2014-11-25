package de.kp.spark.cluster
/* Copyright (c) 2014 Dr. Krusche & Partner PartG
* 
* This file is part of the Spark-FSM project
* (https://github.com/skrusche63/spark-fsm).
* 
* Spark-FSM is free software: you can redistribute it and/or modify it under the
* terms of the GNU General Public License as published by the Free Software
* Foundation, either version 3 of the License, or (at your option) any later
* version.
* 
* Spark-FSM is distributed in the hope that it will be useful, but WITHOUT ANY
* WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR
* A PARTICULAR PURPOSE. See the GNU General Public License for more details.
* You should have received a copy of the GNU General Public License along with
* Spark-FSM. 
* 
* If not, see <http://www.gnu.org/licenses/>.
*/

import akka.actor.{ActorSystem,Props}
import com.typesafe.config.ConfigFactory

import de.kp.spark.core.SparkService
import de.kp.spark.cluster.actor.ClusterMaster

object ClusterService {

  def main(args: Array[String]) {
    
    val name:String = "cluster-server"
    val conf:String = "server.conf"

    val server = new ClusterService(conf, name)
    while (true) {}
    
    server.shutdown
      
  }

}

class ClusterService(conf:String, name:String) extends SparkService {

  val system = ActorSystem(name, ConfigFactory.load(conf))
  sys.addShutdownHook(system.shutdown)
  
  /* Create Spark context */
  private val sc = createCtxLocal("ClusterContext",Configuration.spark)      

  val master = system.actorOf(Props(new ClusterMaster(sc)), name="cluster-master")

  def shutdown = system.shutdown()
  
}