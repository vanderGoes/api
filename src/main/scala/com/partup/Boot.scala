package com.partup

import akka.actor.ActorSystem
import akka.io.IO
import akka.pattern.ask
import akka.util.Timeout
import org.mongodb.scala.MongoClient
import spray.can.Http

import scala.concurrent.duration._

object Boot extends App {

  // we need an ActorSystem to host our application in
  implicit val system = ActorSystem("partup-api")

  val mongo = Option(System.getenv("MONGO_HOST")).map("mongodb://" + _)
    .map(MongoClient(_)).getOrElse(MongoClient())

  // create and start our service actor
  val service = system.actorOf(EventsApiActor.props(mongo), "api-actor")

  implicit val timeout = Timeout(5.seconds)
  // start a new HTTP server on port 8080 with our service actor as the handler
  IO(Http) ? Http.Bind(service, interface = "0.0.0.0", port = 8080)
}
