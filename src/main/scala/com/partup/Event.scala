package com.partup

import spray.httpx.SprayJsonSupport
import spray.json.{JsValue, DefaultJsonProtocol}

case class Event(timestamp: String, eventname: String, payload: JsValue)

object MyJsonProtocol extends DefaultJsonProtocol with SprayJsonSupport {
  implicit val colorFormat = jsonFormat3(Event)
}