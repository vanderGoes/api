package com.partup

import akka.actor.Actor

/**
  * Actor that just eats messages
  */
class NoOpActor extends Actor {
  override def receive = {
    case _ =>
    //noop
  }
}
