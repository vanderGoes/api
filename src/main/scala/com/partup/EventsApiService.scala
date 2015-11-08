package com.partup

import akka.actor.{Actor, ActorRefFactory}
import com.partup.MyJsonProtocol._
import spray.routing._

// we don't implement our route structure directly in the service actor because
// we want to be able to test it independently, without having to spin up an actor
class EventsApiActor extends Actor with EventsApiService {

  def actorRefFactory: ActorRefFactory = context

  // this actor only runs our route, but you could add
  // other things here, like request stream processing
  // or timeout handling
  def receive = runRoute(myRoute)

  def persistEvent(e: RawEvent) = {
    context.actorSelection("../eventLogger") ! e
    context.actorSelection("../eventRouter") ! e
  }
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
  val myRoute =
    path("events") {
      post {
        authorize(isAuthorized) {
          entity(as[RawEvent]) { e =>
            persistEvent(e)
            complete("OK")
          }
        }
      }
    }

  def persistEvent(e: RawEvent): Unit
}
