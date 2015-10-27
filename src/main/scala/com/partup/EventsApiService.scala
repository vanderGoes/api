package com.partup

import java.util.concurrent.TimeUnit

import akka.actor.{Actor, ActorRefFactory, Props}
import com.partup.MyJsonProtocol._
import org.mongodb.scala.MongoClient
import org.mongodb.scala.bson.collection.immutable
import spray.json.pimpAny
import spray.routing._

import scala.concurrent.Await
import scala.concurrent.duration.Duration

// we don't implement our route structure directly in the service actor because
// we want to be able to test it independently, without having to spin up an actor
class EventsApiActor(mongo: MongoClient) extends Actor with EventsApiService {

  def actorRefFactory: ActorRefFactory = context

  // this actor only runs our route, but you could add
  // other things here, like request stream processing
  // or timeout handling
  def receive = runRoute(myRoute)

  def persistEvent(e: Event) = {
    def doc = immutable.Document(e.toJson.compactPrint)

    val obs = mongo
      .getDatabase("events")
      .getCollection("log")
      .insertOne(doc)

    Await.result(obs.toFuture(), Duration(10, TimeUnit.SECONDS))
  }
}

object EventsApiActor {

  def props(mongo: MongoClient): Props = Props(new EventsApiActor(mongo))

}

// this trait defines our service behavior independently from the service actor
trait EventsApiService extends HttpService {

  val authorizationHeader = Option apply System.getenv("PARTUP_API_AUTHKEY") map {
    "Bearer " + _
  }

  val isAuthorized = { requestContext: RequestContext =>
    authorizationHeader.isEmpty ||
      requestContext.request.headers
        .find(_.is("authorization"))
        .exists(authorizationHeader contains _.value)
  }

  def persistEvent(e: Event): Unit

  val myRoute =
    path("events") {
      post {
        authorize(isAuthorized) {
          entity(as[Event]) { e =>
            persistEvent(e)
            complete("OK")
          }
        }
      }
    }
}
