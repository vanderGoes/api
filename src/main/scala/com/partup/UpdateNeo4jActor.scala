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
      if(network_id != null){
        Cypher(
          """MATCH (u {_id:'{creator_id}'},
            |(t {_id: '{network_id}'})
            |CREATE (p:partup {_id:'{_id}',
            |name:'{name}'}),
            |(u)-[:CREATOR_OF]->(p),
            |(t)-[:PART_OF]->(p)
          """).on(("creator_id", creator_id), ("_id", _id), ("name", name), ("network_id", network_id))
          .execute()(conn)
      }else{
        Cypher(
          """MATCH (u {_id:'{creator_id}'}
            |CREATE (p:partup {_id:'{_id}',
            |name:'{name}'}),
            |(u)-[:CREATOR_OF]->(p)
          """).on(("creator_id", creator_id), ("_id", _id), ("name", name))
          .execute()(conn)
      }

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
           |(u)-[:ADMIN_OF]->(t)
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
    case PartnersInsertedEvent(_, _id, partup_id) =>
      Cypher(
        """MATCH (u {_id:'{_id}'),
           |(p {_id:'{partup_id}'})           |
           |OPTIONAL MATCH (u)-[r:SUPPORTER_OF]->(p)
           |CREATE (u)-[z:PARTNER_IN]->(p)
           |SET u.temp_pv = r.pageViews
           |SET z.pageViews = u.temp_pv
           |DELETE r
         """).on(("_id", _id), ("partup_id", partup_id))
        .execute()(conn)

    //Supporters
    case SupportersInsertedEvent(_, _id, partup_id) =>
      Cypher(
        """Match (u {_id:'{_id}'),
           |(p {_id:'{partup_id}'})
           |CREATE (u)-[:SUPPORTER_IN]->(p)
         """).on(("_id", _id), ("partup_id", partup_id))
        .execute()(conn)

    case SupportersRemovedEvent(_, _id, partup_id) =>
      Cypher(
        """MATCH (u {_id:'{_id}'})-[r]->(p {_id:'{partup_id}'})
           |DELETE r
         """).on(("_id", _id), ("partup_id", partup_id))
        .execute()(conn)

    //Members
    case MembersAcceptedEvent(_, _id, tribe_id) =>
      Cypher(
        """MATCH (u {_id:'{_id}'}),
          |(t {_id:'{tribe_id}'})
          |CREATE (u)-[:MEMBER_OF]->(t)
        """).on(("_id", _id), ("tribe_id", tribe_id))
        .execute()(conn)

    case MembersInsertedEvent(_, _id, tribe_id) =>
      Cypher(
        """MATCH (u {_id:'{_id}'}),
          |(t {_id:'{tribe_id}'})
          |CREATE (u)-[:MEMBER_OF]->(t)
        """).on(("_id", _id), ("tribe_id", tribe_id))
        .execute()(conn)

    case MembersRemovedEvent(_, _id, tribe_id) =>
      Cypher(
        """MATCH (u {_id:'{_id'})-[r]->(t {_id:'{tribe_id}'})
          |DELETE r
        """).on(("_id", _id), ("tribe_id", tribe_id))
        .execute()(conn)

    //Analytics
    case AnalyticsPageViewEvent(_, _id, partup_id) =>
      Cypher(
        """MATCH (u {_id:'{_id}'},
          |(p {partup_id:'{'partup_id}'}),
          |(u)-[r]->(p)
          |SET r.pageViews=r.pageViews+1
        """).on(("_id", _id), ("partup_id", partup_id))
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
