package de.kp.spark.cluster.api
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

import java.util.Date

import org.apache.spark.SparkContext

import akka.actor.{ActorRef,ActorSystem,Props}
import akka.pattern.ask

import akka.util.Timeout

import spray.http.StatusCodes._

import spray.routing.{Directives,HttpService,RequestContext,Route}

import scala.concurrent.{ExecutionContext}
import scala.concurrent.duration.Duration
import scala.concurrent.duration.DurationInt

import scala.util.parsing.json._

import de.kp.spark.core.model._
import de.kp.spark.core.rest.RestService

import de.kp.spark.cluster.Configuration

import de.kp.spark.cluster.actor.ClusterMaster
import de.kp.spark.cluster.model._

class RestApi(host:String,port:Int,system:ActorSystem,@transient val sc:SparkContext) extends HttpService with Directives {

  implicit val ec:ExecutionContext = system.dispatcher  
  import de.kp.spark.core.rest.RestJsonSupport._
  
  override def actorRefFactory:ActorSystem = system

  val (duration,retries,time) = Configuration.actor   
  val master = system.actorOf(Props(new ClusterMaster(sc)), name="similarity-master")

  def start() {
    RestService.start(routes,system,host,port)
  }

  /*
   * The routes defines the different access channels this API supports
   */
  private def routes:Route = {

    path("get" / Segment) {subject => 
	  post {
	    respondWithStatus(OK) {
	      ctx => doGet(ctx,subject)
	    }
	  }
    }  ~ 
    path("index" / Segment) {subject =>  
	  post {
	    respondWithStatus(OK) {
	      ctx => doIndex(ctx,subject)
	    }
	  }
    }  ~ 
    path("register" / Segment) {subject =>  
	  post {
	    respondWithStatus(OK) {
	      ctx => doRegister(ctx,subject)
	    }
	  }
    }  ~ 
    path("status") { 
	  post {
	    respondWithStatus(OK) {
	      ctx => doStatus(ctx)
	    }
	  }
    }  ~ 
    path("track" / Segment) {subject => 
	  post {
	    respondWithStatus(OK) {
	      ctx => doTrack(ctx,subject)
	    }
	  }
    }  ~      
    path("train") {
	  post {
	    respondWithStatus(OK) {
	      ctx => doTrain(ctx)
	    }
	  }
    }
  }

  private def doGet[T](ctx:RequestContext,subject:String) = {
	    
    subject match {

      case "feature" => doRequest(ctx,"cluster","get:feature")
      
	  case "sequence" => doRequest(ctx,"cluster","get:sequence")
	      
	  case _ => {}
	  
    }
    
  }

  private def doIndex[T](ctx:RequestContext,subject:String) = {
	    
    subject match {

      case "feature" => doRequest(ctx,"cluster","index:feature")
      
	  case "sequence" => doRequest(ctx,"cluster","index:sequence")
	      
	  case _ => {}
	  
    }
    
  }

  private def doRegister[T](ctx:RequestContext,subject:String) = {
 
    subject match {

      case "feature" => doRequest(ctx,"cluster","register:feature")
      
	  case "sequence" => doRequest(ctx,"cluster","register:sequence")
	      
	  case _ => {}
	  
    }

  }

  private def doTrack[T](ctx:RequestContext,subject:String) = {
	    
    subject match {

      case "feature" => doRequest(ctx,"cluster","track:feature")
      
	  case "sequence" => doRequest(ctx,"cluster","track:sequence")
	      
	  case _ => {}
	  
    }
    
  }

  private def doTrain[T](ctx:RequestContext) = doRequest(ctx,"cluster","train")

  private def doStatus[T](ctx:RequestContext) = doRequest(ctx,"cluster","status")
  
  private def doRequest[T](ctx:RequestContext,service:String,task:String="train") = {
     
    val request = new ServiceRequest(service,task,getRequest(ctx))
    implicit val timeout:Timeout = DurationInt(time).second
    
    val response = ask(master,request).mapTo[ServiceResponse] 
    ctx.complete(response)
    
  }

  private def getHeaders(ctx:RequestContext):Map[String,String] = {
    
    val httpRequest = ctx.request
    
    /* HTTP header to Map[String,String] */
    val httpHeaders = httpRequest.headers
    
    Map() ++ httpHeaders.map(
      header => (header.name,header.value)
    )
    
  }
 
  private def getBodyAsMap(ctx:RequestContext):Map[String,String] = {
   
    val httpRequest = ctx.request
    val httpEntity  = httpRequest.entity    

    val body = JSON.parseFull(httpEntity.data.asString) match {
      case Some(map) => map
      case None => Map.empty[String,String]
    }
      
    body.asInstanceOf[Map[String,String]]
    
  }
  
  private def getRequest(ctx:RequestContext):Map[String,String] = {

    val headers = getHeaders(ctx)
    val body = getBodyAsMap(ctx)
    
    headers ++ body
    
  }

}