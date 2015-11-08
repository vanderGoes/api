package com.partup

import akka.actor.{Actor, Props}
import spray.json.DefaultJsonProtocol._

/**
  * Unpacks RawEvents and sends them to the proper receiver.
  */
class EventRoutingActor extends Actor {
  override def receive: Receive = {
    case event: RawEvent =>
      event.eventname match {
        case "partups.inserted" =>
          val payload = event.payload.asJsObject.fields
          val id = payload("0").convertTo[String]
          val partup = payload("1").asJsObject.fields
          val name = partup("name").convertTo[String]
          val creator = partup("creator_id").convertTo[String]

          val createdEvent = PartupCreatedEvent(event.timestamp, name, id, creator)

          context.actorOf(Props[UpdateNeo4jActor]) ! createdEvent

        case "partups.updated" =>


        case _ =>
        // Not routing events I don't know
      }
  }
}
