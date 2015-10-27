package com.partup

import org.specs2.mutable.Specification
import spray.testkit.Specs2RouteTest

class EventsApiServiceSpec extends Specification with Specs2RouteTest with EventsApiService {
  def actorRefFactory = system
  def persistEvent(e: RawEvent) = {}
  
/*
  "MyService" should {

    "return a greeting for GET requests to the root path" in {
      Get() ~> myRoute ~> check {
        responseAs[String] must contain("Say hello")
      }
    }

    "leave GET requests to other paths unhandled" in {
      Get("/kermit") ~> myRoute ~> check {
        handled must beFalse
      }
    }

    "return a MethodNotAllowed error for PUT requests to the root path" in {
      Put() ~> sealRoute(myRoute) ~> check {
        status === MethodNotAllowed
        responseAs[String] === "HTTP method not allowed, supported methods: GET"
      }
    }
  }
*/
}
