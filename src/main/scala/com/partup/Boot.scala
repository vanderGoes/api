package com.partup

import akka.actor.SupervisorStrategy.Restart
import akka.actor.{ActorSystem, OneForOneStrategy, Props}
import akka.io.IO
import akka.pattern.ask
import akka.routing.SmallestMailboxPool
import akka.util.Timeout
import org.mongodb.scala.MongoClient
import spray.can.Http

import scala.concurrent.duration._

object Boot extends App {

  // we need an ActorSystem to host our application in
  implicit val system = ActorSystem("partup-api")

  createEventLoggingActor
  createEventRoutingActor

  val service = system.actorOf(Props[EventsApiActor])
  implicit val timeout = Timeout(5.seconds)

  def createEventRoutingActor = {
    val neo4jHost = Option(System.getenv("NEO4J_HOST"))
    if (neo4jHost.isDefined) {
      val strat = OneForOneStrategy() { case e => Restart }
      val router = SmallestMailboxPool(nrOfInstances = 10, supervisorStrategy = strat)
      system.actorOf(router.props(Props[EventRoutingActor]), "eventRouter")
    } else {
      println("NEO4J_HOST env not set, not writing events to Neo4J.")
      system.actorOf(Props[NoOpActor], "eventRouter")
    }
  }

  def createEventLoggingActor = {
    val mongoHost = Option(System.getenv("MONGO_HOST"))
    if (mongoHost.isDefined) {
      val mongo = MongoClient(s"mongodb://${mongoHost.get}")
      system.actorOf(EventLoggingActor.props(mongo), "eventLogger")
    } else {
      println("MONGO_HOST env not set, not logging events.")
      system.actorOf(Props[NoOpActor], "eventLogger")
    }
  }

  // start a new HTTP server on port 8080 with our service actor as the handler
  IO(Http) ? Http.Bind(service, interface = "0.0.0.0", port = 8080)
}
