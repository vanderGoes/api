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
        //Part-ups
        case "partups.inserted" =>
          val payload = event.payload.asJsObject.fields
          val creator_id = payload("0").convertTo[String]
          val partup = payload("1").asJsObject.fields
          val _id = partup("_id").convertTo[String]
          val name = partup("name").convertTo[String]
          val network_id = partup("network_id").convertTo[String]

          val createdEvent = PartupsInsertedEvent(event.timestamp, creator_id, _id, name, network_id)

          context.actorOf(Props[UpdateNeo4jActor]) ! createdEvent

        case "partups.updated" =>
          val payload = event.payload.asJsObject.fields
          val userID = payload("0").convertTo[String]
          val partup = payload("1").asJsObject.fields
          val _id = partup("_id").convertTo[String]
          val name = partup("name").convertTo[String]

          val createdEvent = PartupsUpdatedEvent(event.timestamp, _id, name)

          context.actorOf(Props[UpdateNeo4jActor]) ! createdEvent

        case "partups.changed" =>
          val payload = event.payload.asJsObject.fields
          val userID = payload("0").convertTo[String]
          val partup = payload("1").asJsObject.fields
          val _id = partup("_id").convertTo[String]
          val name = partup("name").convertTo[String]

          val createdEvent = PartupsChangedEvent(event.timestamp, _id, name)

          context.actorOf(Props[UpdateNeo4jActor]) ! createdEvent

        case "partups.removed" =>
          val payload = event.payload.asJsObject.fields
          val userID = payload("0").convertTo[String]
          val partup = payload("1").asJsObject.fields
          val _id = partup("_id").convertTo[String]

          val createdEvent = PartupsRemovedEvent(event.timestamp, _id)

          context.actorOf(Props[UpdateNeo4jActor]) ! createdEvent

        //Tribes
        case "tribes.inserted" =>
          val payload = event.payload.asJsObject.fields
          val userID = payload("0").convertTo[String]
          val tribe = payload("1").asJsObject.fields
          val _id = tribe("_id").convertTo[String]
          val name = tribe("name").convertTo[String]
          val tribe_id = tribe("network_id").convertTo[String]
          val admin_id = tribe("admin_id").convertTo[String]

          val createdEvent = TribesInsertedEvent(event.timestamp, _id, name, tribe_id, admin_id)

          context.actorOf(Props[UpdateNeo4jActor]) ! createdEvent

        case "tribes.updated" =>
          val payload = event.payload.asJsObject.fields
          val userID = payload("0").convertTo[String]
          val tribes = payload("1").asJsObject.fields
          val _id = tribes("_id").convertTo[String]
          val name = tribes("name").convertTo[String]

          val createdEvent = TribesUpdatedEvent(event.timestamp, _id, name)

          context.actorOf(Props[UpdateNeo4jActor]) ! createdEvent

        case "tribes.changed" =>
          val payload = event.payload.asJsObject.fields
          val userID = payload("0").convertTo[String]
          val tribe = payload("1").asJsObject.fields
          val _id = tribe("_id").convertTo[String]
          val name = tribe("name").convertTo[String]

          val createdEvent = TribesChangedEvent(event.timestamp, _id, name)

          context.actorOf(Props[UpdateNeo4jActor]) ! createdEvent

        case "tribes.removed" =>
          val payload = event.payload.asJsObject.fields
          val userID = payload("0").convertTo[String]
          val tribe = payload("1").asJsObject.fields
          val _id = tribe("_id").convertTo[String]

          val createdEvent = TribesRemovedEvent(event.timestamp, _id)

          context.actorOf(Props[UpdateNeo4jActor]) ! createdEvent

        //Users
        case "user.inserted" =>
          val payload = event.payload.asJsObject.fields
          val userID = payload("0").convertTo[String]
          val user = payload("1").asJsObject.fields
          val _id = user("_id").convertTo[String]
          val profile = user("profile").asJsObject.fields
          val name = profile("name").convertTo[String]

          val createdEvent = UsersInsertedEvent(event.timestamp, _id, name)

          context.actorOf(Props[UpdateNeo4jActor]) ! createdEvent

        case "user.updated" =>
          val payload = event.payload.asJsObject.fields
          val userID = payload("0").convertTo[String]
          val user = payload("1").asJsObject.fields
          val _id = user("_id").convertTo[String]
          val profile = user("profile").asJsObject.fields
          val name = profile("name").convertTo[String]

          val createdEvent = TribesUpdatedEvent(event.timestamp, _id, name)

          context.actorOf(Props[UpdateNeo4jActor]) ! createdEvent

        case "user.changed" =>
          val payload = event.payload.asJsObject.fields
          val userID = payload("0").convertTo[String]
          val user = payload("1").asJsObject.fields
          val _id = user("_id").convertTo[String]
          val profile = user("profile").asJsObject.fields
          val name = profile("name").convertTo[String]

          val createdEvent = UsersChangedEvent(event.timestamp, _id, name)

          context.actorOf(Props[UpdateNeo4jActor]) ! createdEvent

        //Partners
        case "partups.uppers.inserted" =>
          val payload = event.payload.asJsObject.fields
          val partup_id = payload("0").convertTo[String]
          val user_id = payload("1").convertTo[String]

          val createdEvent = PartnersInsertedEvent(event.timestamp, user_id, partup_id)

          context.actorOf(Props[UpdateNeo4jActor]) ! createdEvent

        //Supporters
        case "partups.supporters.inserted" =>
          val payload = event.payload.asJsObject.fields
          val partup = payload("0").asJsObject.fields
          val partup_id = partup("_id").convertTo[String]
          val user = payload("1").asJsObject.fields
          val user_id = user("_id").convertTo[String]

          val createdEvent = SupportersInsertedEvent(event.timestamp, user_id, partup_id)

          context.actorOf(Props[UpdateNeo4jActor]) ! createdEvent

        case "partups.supporters.removed" =>
          val payload = event.payload.asJsObject.fields
          val partup = payload("0").asJsObject.fields
          val partup_id = partup("_id").convertTo[String]
          val user = payload("1").asJsObject.fields
          val user_id = user("_id").convertTo[String]

          val createdEvent = SupportersRemovedEvent(event.timestamp, user_id, partup_id)

          context.actorOf(Props[UpdateNeo4jActor]) ! createdEvent

        //Member
        case "networks.accepted" =>
          val payload = event.payload.asJsObject.fields
          val tribe_user_id = payload("0").convertTo[String]
          val tribe_id = payload("1").convertTo[String]
          val _id = payload("2").convertTo[String]

          val createdEvent = MembersInsertedEvent(event.timestamp, _id, tribe_id)

          context.actorOf(Props[UpdateNeo4jActor]) ! createdEvent

        case "network.uppers.inserted" =>
          val payload = event.payload.asJsObject.fields
          val _id = payload("0").convertTo[String]
          val tribe_id = payload("1").convertTo[String]

          val createdEvent = MembersInsertedEvent(event.timestamp, _id, tribe_id)

          context.actorOf(Props[UpdateNeo4jActor]) ! createdEvent

        case "networks.uppers.remove" =>
          val payload = event.payload.asJsObject.fields
          val _id = payload("0").convertTo[String]
          val tribe_id = payload("1").convertTo[String]

          val createdEvent = MembersRemovedEvent(event.timestamp, _id, tribe_id)

          context.actorOf(Props[UpdateNeo4jActor]) ! createdEvent

        //Analytics
        case "partups.analytics.click" =>
          val payload = event.payload.asJsObject.fields
          val partup_id = payload("0").convertTo[String]
          val _id = payload("1").convertTo[String]

          val createdEvent = AnalyticsPageViewEvent(event.timestamp, _id, partup_id)

          context.actorOf(Props[UpdateNeo4jActor]) ! createdEvent

        case _ =>
        // Not routing events I don't know
      }
  }
}
