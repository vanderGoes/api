package com.partup

import spray.httpx.SprayJsonSupport
import spray.json.{JsValue, DefaultJsonProtocol}

sealed abstract class Event(timestamp: String, eventname: String)

case class RawEvent(timestamp: String, eventname: String, payload: JsValue) extends Event(timestamp, eventname)

case class PartupCreatedEvent(timestamp: String, name: String, id: String, creator: String)
  extends Event(timestamp, "partups.inserted")

object MyJsonProtocol extends DefaultJsonProtocol with SprayJsonSupport {
  implicit val colorFormat = jsonFormat3(RawEvent)
}
