package com.partup

import akka.actor.Actor
import org.anormcypher.{Neo4jREST, _}

/**
 * Updates the graph database based on the events received from Meteor
 */
class UpdateNeo4jActor(conn: Neo4jREST) extends Actor {

  override def receive = {
    //Part-ups
    case PartupsInsertedEvent(_, creator_id, _id, name, network_id) =>
      Cypher(
        """MATCH (u {_id:'{creator_id}'},
           |(t {_id: '{network_id}'})
           |CREATE (p:partup {_id:'{_id}',
           |name:'{name}'}),
           |[u]-[r1:CREATOR_OF]->[p],
           |[t]-[r2:PART_OF]->[p]
         """).on(("creator_id", creator_id), ("_id", _id), ("name", name), ("network_id", network_id))
          .execute()(conn)

    case PartupsUpdatedEvent(_, _id, name) =>
      Cypher(
        """MATCH (p {_id:'{_id}'})
           |SET p.name:'{name}'
         """).on(("_id", _id), ("name", name))
        .execute()(conn)

    case PartupsChangedEvent(_, _id, name) =>
      Cypher(
        """MATCH (p {_id:'{_id}'})
           |SET p.name:'{name}'
         """).on(("_id", _id), ("name", name))
        .execute()(conn)

    case PartupsRemovedEvent(_, _id) =>
      Cypher(
        """MATCH (p {_id:'{_id}'})
           |DETACH DELETE p
         """).on(("_id", _id))
        .execute()(conn)

    //Tribes
    case TribesInsertedEvent(_, _id, name, network_id, admin_id) =>
      Cypher(
        """MATCH (u {_id:'{admin_id}'})
           |CREATE (t:tribe {_id:'{_id}',
           |name:'{name}'}),
           |[u]-[r:ADMIN_OF]->[t]
         """).on(("_id", _id), ("name", name), ("network_id", network_id), ("admin_id", admin_id))
        .execute()(conn)

    case TribesUpdatedEvent(_, _id, name) =>
      Cypher(
        """MATCH (t {_id:'{_id}'})
           |SET t.name:'{name}'
         """).on(("_id", _id), ("name", name))
        .execute()(conn)

    case TribesChangedEvent(_, _id, name) =>
      Cypher(
        """MATCH (t {_id:'{_id}'})
           |SET t.name:'{name}'
         """).on(("_id", _id), ("name", name))
        .execute()(conn)

    case TribesRemovedEvent(_, _id) =>
      Cypher(
        """MATCH (t {_id:'{_id}'})
           |DETACH DELETE t
         """).on(("_id", _id))
        .execute()(conn)

    //Users
    case UsersInsertedEvent(_, _id, name) =>
      Cypher(
        """CREATE (u:user {_id:'{_id}',
           |name:'{name}')
         """).on(("_id", _id), ("name", name))
        .execute()(conn)

    case UsersUpdatedEvent(_, _id, name) =>
      Cypher(
        """MATCH (u {_id:'{_id}'})
           |SET u.name:'{name}'
         """).on(("_id", _id), ("name", name))
        .execute()(conn)

    case UsersChangedEvent(_, _id, name) =>
      Cypher(
        """MATCH (u {_id:'{_id}'})
           |SET u.name:'{name}'
         """).on(("_id", _id), ("name", name))
        .execute()(conn)

    //Partners
    case PartnersInsertedEvent(_, partup_id, user_id) =>
      Cypher(
        """MATCH (p {_id:'{partup_id}'}),
           |(u {_id:'{user_id}')
           |CREATE [u]-[r:PARTNER_IN]->[p]
         """).on(("partup_id", partup_id), ("user_id", user_id))
        .execute()(conn)

    //Supporters
    case SupportersInsertedEvent(_, partup_id, user_id) =>
      Cypher(
        """Match (p {_id:'{partup_id}'}),
           |(u {_id:'{user_id}')
           |CREATE [u]-[r:SUPPORTER_IN]->[p]
         """).on(("_id", partup_id), ("user_id", user_id))
        .execute()(conn)

    case SupportersRemovedEvent(_, partup_id, user_id) =>
      Cypher(
        """MATCH (p {_id:'{partup_id}'}),
           |(u {_id:'{user_id}'}),
           |[u]-[r:SUPPORTER_OF]->[p]
           |DELETE r
         """).on(("partup_id", partup_id), ("user_id", user_id))
        .execute()(conn)

    //Invitation
    case PartupsInvitedEvent(_, partup_id, user_id) =>
      Cypher(
        """MATCH (p {_id:'{partup_id}'}),
           |(u {_id:'{user_id}'})
           |CREATE [u]-[r:INVITED_FOR]->[p]
         """).on(("partup_id", partup_id), ("user_id", user_id))
        .execute()(conn)

    //Members
    case MembersInvitedEvent(_, tribe_id, user_id) =>
      Cypher(
        """MATCH(u {_id:'{user_id}'}),
           |(t {_id:'{tribe_id}'})
           |CREATE [u]-[r:INVITED_MEMBER_OF]->[t]
         """).on(("tribe_id", tribe_id), ("user_id", user_id))
        .execute()(conn)

    case MembersPendingEvent(_, tribe_id, user_id) =>
      Cypher(
        """MATCH (t {_id:'{tribe_id}'}),
          |(u {_id:'{user_id}'})
          |CREATE [u]-[r:PENDING_MEMBER_OF]->[t]
        """).on(("tribe_id", tribe_id), ("user_id", user_id))
        .execute()(conn)

    case MembersAcceptedEvent(_, tribe_id, user_id) =>
      Cypher(
        """MATCH (u {_id:'${membersAccepted.user_id}'})-[r1]->(t {_id:'${membersAccepted.tribe_id}'})
           |CREATE [u]-[r2:MEMBER_OF]->[t]
           |DELETE r1
         """).on(("tribe_id", tribe_id), ("user_id", user_id))
        .execute()(conn)
  }

  def this() = this(
    Neo4jREST(
      host = Option(System.getenv("NEO4J_HOST")).getOrElse("localhost"),
      port = Option(System.getenv("NEO4J_PORT")).map(_.toInt).getOrElse(7474),
      username = Option(System.getenv("NEO4J_USER")).getOrElse(""),
      password = Option(System.getenv("NEO4J_PASSWORD")).getOrElse("")
    )
  )

}
