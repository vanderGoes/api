package com.partup

import akka.actor.Actor
import org.anormcypher.{Neo4jREST, _}

/**
  * Updates the graph database based on the events received from Meteor
  */
class UpdateNeo4jActor(connection: Neo4jREST) extends Actor {

  def this() = this(
    Neo4jREST(
      host = Option(System.getenv("NEO4J_HOST")).getOrElse("localhost"),
      port = Option(System.getenv("NEO4J_PORT")).map(_.toInt).getOrElse(7474),
      username = Option(System.getenv("NEO4J_USER")).getOrElse(""),
      password = Option(System.getenv("NEO4J_PASSWORD")).getOrElse("")
    )
  )

  override def receive = {
    case PartupCreatedEvent(_, name, id, _) =>
      Cypher("CREATE (n:partup{name:'{name}', id:'{id}'})")
        .on(("name", name), ("id", id))
        .execute()(connection)
  }
}
