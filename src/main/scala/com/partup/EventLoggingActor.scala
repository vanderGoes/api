package com.partup

import akka.actor.{Props, Actor}
import org.mongodb.scala.MongoClient

/**
  * Actor that logs events to mongo if a Mongo connection
  */
class EventLoggingActor(mongo: MongoClient) extends Actor {
  override def receive = {
    case e: Event =>
      context.actorOf(MongoEventLoggingActor.props(mongo)) ! e
  }
}

object EventLoggingActor {

  def props(mongo: MongoClient) = Props(new EventLoggingActor(mongo))

}