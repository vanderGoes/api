package com.partup

import spray.httpx.SprayJsonSupport
import spray.json.{JsValue, DefaultJsonProtocol}

sealed abstract class Event(timestamp: String, eventname: String)

case class RawEvent(timestamp: String, eventname: String, payload: JsValue) extends Event(timestamp, eventname)

object MyJsonProtocol extends DefaultJsonProtocol with SprayJsonSupport {
  implicit val colorFormat = jsonFormat3(RawEvent)
}