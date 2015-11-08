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
    //Part-ups
    case partupsInserted: PartupsInsertedEvent =>
      Cypher(
        s"""MATCH (u {_id:'${partupsInserted.creator_id}'},
           |(t {_id: '${partupsInserted.network_id}'})
           |CREATE (p:partup {_id:'${partupsInserted._id}',
           |name:'${partupsInserted.name}'}),
           |[u]-[r1:CREATOR_OF]->[p],
           |[t]-[r2:PART_OF]->[p]
         """.stripMargin).execute()(connection)

    case partupsUpdated: PartupsUpdatedEvent =>
      Cypher(
        s"""MATCH (p {_id:'${partupsUpdated._id}'})
           |SET p.name:'${partupsUpdated.name}'
         """.stripMargin).execute()(connection)

    case partupsChanged: PartupsChangedEvent =>
      Cypher(
        s"""MATCH (p {_id:'${partupsChanged._id}'})
           |SET p.name:'${partupsChanged.name}'
         """.stripMargin).execute()(connection)

    case partupsRemoved: PartupsRemovedEvent =>
      Cypher(
        s"""MATCH (p {_id:'${partupsRemoved._id}'})
           |DETACH DELETE p
         """.stripMargin).execute()(connection)

    //Tribes
    case tribesInserted: TribesInsertedEvent =>
      Cypher(
        s"""MATCH (u {_id:'${tribesInserted.admin_id}'})
           |CREATE (t:tribe {_id:'${tribesInserted._id}',
           |name:'${tribesInserted.name}'}),
           |[u]-[r:ADMIN_OF]->[t]
         """.stripMargin).execute()(connection)

    case tribesUpdated: TribesUpdatedEvent =>
      Cypher(
        s"""MATCH (t {_id:'${tribesUpdated._id}'})
           |SET t.name:'${tribesUpdated.name}'
         """.stripMargin).execute()(connection)

    case tribesChanged: TribesChangedEvent =>
      Cypher(
        s"""MATCH (t {_id:'${tribesChanged._id}'})
           |SET t.name:'${tribesChanged.name}'
         """.stripMargin).execute()(connection)

    case tribesRemoved: TribesRemovedEvent =>
      Cypher(
        s"""MATCH (t {_id:'${tribesRemoved._id}'})
           |DETACH DELETE t
         """.stripMargin).execute()(connection)

    //Users
    case usersInserted: UsersInsertedEvent =>
      Cypher(
        s"""CREATE (u:user {_id:'${usersInserted._id}',
           |name:'${usersInserted.name}')
         """.stripMargin).execute()(connection)

    case usersUpdated: UsersUpdatedEvent =>
      Cypher(
        s"""MATCH (u {_id:'${usersUpdated._id}'})
           |SET u.name:'${usersUpdated.name}'
         """.stripMargin).execute()(connection)

    case usersChanged: UsersChangedEvent =>
      Cypher(
        s"""MATCH (u {_id:'${usersChanged._id}'})
           |SET u.name:'${usersChanged.name}'
         """.stripMargin).execute()(connection)

    //Partners
    case partnersInserted: PartnersInsertedEvent =>
      Cypher(
        s"""MATCH (p {_id:'${partnersInserted.partup_id}'}),
           |(u {_id:'${partnersInserted.user_id}')
           |CREATE [u]-[r:PARTNER_IN]->[p]
         """.stripMargin).execute()(connection)

    //Supporters
    case supportersInserted: SupportersInsertedEvent =>
      Cypher(
        s"""Match (p {_id:'${supportersInserted.partup_id}'}),
           |(u {_id:'${supportersInserted.user_id}')
           |CREATE [u]-[r:SUPPORTER_IN]->[p]
         """.stripMargin).execute()(connection)

    case supportersInvited: SupportersInsertedEvent =>
      Cypher(
        s"""MATCH (p {_id:'${supportersInvited.partup_id}'}),
           |(u {_id:'${supportersInvited.user_id}'})
           |CREATE [u]-[r:INVITED_SUPPORTER_OF]->[p]
         """.stripMargin).execute()(connection)

    case supportersRemoved: SupportersRemovedEvent =>
      Cypher(
        s"""MATCH (p {_id:'${supportersRemoved.partup_id}'}),
           |(u {_id:'${supportersRemoved.user_id}'}),
           |[u]-[r:SUPPORTER_OF]->[p]
           |DELETE r
         """.stripMargin).execute()(connection)

    //Members
    case membersInvitation: MembersInvitedEvent =>
      Cypher(
        s"""MATCH(u {_id:'${membersInvitation.user_id}'}),
           |(t {_id:'${membersInvitation.tribe_id}'})
           |CREATE [u]-[r:INVITED_MEMBER_OF]->[t]
         """.stripMargin).execute()(connection)

    case membersPending: MembersPendingEvent =>
      Cypher(
        s"""MATCH (t {_id:'${membersPending.tribe_id}'}),
          |(u {_id:'${membersPending.user_id}'})
          |CREATE [u]-[r:PENDING_MEMBER_OF]->[t]
        """.stripMargin).execute()(connection)

    case membersAccepted: MembersAcceptedEvent =>
      Cypher(
        s"""MATCH (u {_id:'${membersAccepted.user_id}'})-[r1]->(t {_id:'${membersAccepted.tribe_id}'})
           |CREATE [u]-[r2:MEMBER_OF]->[t]
           |DELETE r1
         """.stripMargin).execute()(connection)
  }
}
