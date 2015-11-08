package com.partup

import java.util.concurrent.TimeUnit

import akka.actor.{Actor, Props}
import org.mongodb.scala.MongoClient
import org.mongodb.scala.bson.collection.immutable.Document
import com.partup.MyJsonProtocol._
import spray.json.pimpAny

import scala.concurrent.Await
import scala.concurrent.duration.Duration

/**
  * Logs all events to MongoDB with their raw payload
  */
class EventLoggingActor(mongo: MongoClient) extends Actor {
  override def receive: Receive = {
    case e: RawEvent =>

      val doc = Document(e.toJson.compactPrint)

      val obs = mongo
        .getDatabase("events")
        .getCollection("log")
        .insertOne(doc)

      Await.result(obs.toFuture(), Duration(2, TimeUnit.SECONDS))
  }
}

object EventLoggingActor {

  def props(mongo: MongoClient): Props = Props(new EventLoggingActor(mongo))

}