package com.partup

import spray.httpx.SprayJsonSupport
import spray.json.{JsValue, DefaultJsonProtocol}

sealed abstract class Event(timestamp: String, eventname: String)

case class RawEvent(timestamp: String, eventname: String, payload: JsValue) extends Event(timestamp, eventname)

//Part-ups
case class PartupsInsertedEvent(timestamp: String, creator_id: String, _id: String, name: String, tags: List, language: String, place_id: String, country: String, network_id: String, privacy_type: Int, activity_count: Int, progress: Int, type_partup: String, type_com_budget: Int, type_org_budget: Int, phase: String)
  extends Event(timestamp, "partups.inserted")

case class PartupsUpdatedEvent(timestamp: String, _id: String, name: String, tags: List, language: String, place_id: String, country: String, privacy_type: Int, activity_count: Int, progress: Int, type_partup: String, type_com_budget: Int, type_org_budget: Int, phase: String)
  extends Event(timestamp, "partups.updated")

case class PartupsChangedEvent(timestamp: String, _id: String, name: String, tags: List, language: String, place_id: String, country: String, privacy_type: Int, activity_count: Int, progress: Int, type_partup: String, type_com_budget: Int, type_org_budget: Int, phase: String)
  extends Event(timestamp, "partups.changed")

case class PartupsRemovedEvent(timestamp: String, _id: String)
  extends Event(timestamp, "partups.removed")

//Tribes
case class TribesInsertedEvent(timestamp: String, _id: String, name: String, privacy_type: String, admin_id: String)
  extends Event(timestamp, "tribes.inserted")

case class TribesUpdatedEvent(timestamp: String, _id: String, name: String, privacy_type: String)
  extends Event(timestamp, "tribes.updated")

case class TribesChangedEvent(timestamp: String, _id: String, name: String, privacy_type: String)
  extends Event(timestamp, "tribes.changed")

case class TribesRemovedEvent(timestamp: String, _id: String)
  extends Event(timestamp, "tribes.removed")

//Users
case class UsersInsertedEvent(timestamp: String, _id: String, name: String, place_id: String, country: String, tags: List)
  extends Event(timestamp, "users.inserted")

case class UsersUpdatedEvent(timestamp: String, _id: String, name: String, place_id: String, country: String, tags: List)
  extends Event(timestamp, "users.updated")

case class UsersChangedEvent(timestamp: String, _id: String, name: String, place_id: String, country: String, tags: List)
  extends Event(timestamp, "users.changed")

//Partners
case class PartnersInsertedEvent(timestamp: String, _id: String, partup_id: String)
  extends Event(timestamp, "partups.uppers.inserted")

//Supporters
case class SupportersInsertedEvent(timestamp: String, _id: String, partup_id: String)
  extends Event(timestamp, "supporters.inserted")

case class SupportersRemovedEvent(timestamp: String, _id: String, partup_id: String)
  extends Event(timestamp, "supporters.removed")

//Members
case class MembersAcceptedEvent(timestamp: String, _id: String, tribe_id: String)
  extends Event(timestamp, "networks.accepted")

case class MembersInsertedEvent(timestamp: String, _id: String, tribe_id: String)
  extends Event(timestamp, "networks.accepted")

case class MembersRemovedEvent(timestamp: String, _id: String, tribe_id:String)
  extends Event(timestamp, "networks.uppers.remove")

//Analytics
case class AnalyticsPageViewEvent(timestamp: String, _id: String, partup_id: String)
  extends Event(timestamp, "partups.analytics.click")

//Contributions
case class ContributionsInsertedEvent(timestamp: String, _id: String, partup_id: String)
  extends Event(timestamp, "contributions.inserted")

//Comments
case class CommentsInsertedEvent(timestamp: String, _id: String, partup_id: String)
  extends Event(timestamp, "updates.comments.inserted")

object MyJsonProtocol extends DefaultJsonProtocol with SprayJsonSupport {
  implicit val colorFormat = jsonFormat3(RawEvent)
}
