package com.partup

import akka.actor.Actor
import org.anormcypher.{Neo4jREST, _}

/**
  * Updates the graph database based on the events received from Meteor
  */
class UpdateNeo4jActor(conn: Neo4jREST) extends Actor {

  override def receive = {
    //Part-ups
    case PartupsInsertedEvent(_, creator_id, _id, name, tags, language, place_id, country, network_id, privacy_type, activity_count, progress, type_partup, type_com_budget, type_org_budget, phase) =>
      if(network_id != null){
        Cypher(
          """MATCH (u:User {_id:'{creator_id}'}),
            |(t:Tribe {_id: '{network_id}'})
            |CREATE (p:Partup {_id:'{_id}',
            |name:'{name}',
            |tags:[{tags}],
            |language:'{language}',
            |place_id:'{place_id}',
            |country:'{country}',
            |privacy_type:{privacy_type},
            |activity_count:{activity_count},
            |progress:{progress},
            |type_partup:'{type_partup}',
            |type_com_budget:{type_com_budget},
            |type_org_budget:{type_org_budget},
            |phase:'{phase}'}),
            |(u)-[:CREATOR_IN]->(p),
            |(p)-[:PART_OF]->(t)
          """).on(("creator_id", creator_id), ("_id", _id), ("name", name), ("tags", tags), ("language", language), ("place_id", place_id), ("country", country), ("network_id", network_id), ("privacy_type", privacy_type), ("activity_count", activity_count), ("progress", progress), ("type_partup", type_partup), ("type_com_budget", type_com_budget), ("type_org_budget", type_org_budget), ("phase", phase))
          .execute()(conn)
      }else{
        Cypher(
          """MATCH (u:User {_id:'{creator_id}'})
            |CREATE (p:Partup {_id:'{_id}',
            |name:'{name}',
            |tags:[{tags}],
            |language:'{language}',
            |place_id:'{place_id}',
            |country:'{country}',
            |privacy_type:{privacy_type},
            |activity_count:{activity_count},
            |progress:{progress},
            |type_partup:'{type_partup}',
            |type_com_budget:{type_com_budget},
            |type_org_budget:{type_org_budget},
            |phase:'{phase}'}),
            |(u)-[:CREATOR_IN]->(p)
          """).on(("creator_id", creator_id), ("_id", _id), ("name", name), ("tags", tags), ("language", language), ("place_id", place_id), ("country", country), ("privacy_type", privacy_type), ("activity_count", activity_count), ("progress", progress), ("type_partup", type_partup), ("type_com_budget", type_com_budget), ("type_org_budget", type_org_budget), ("phase", phase))
          .execute()(conn)
      }

    case PartupsUpdatedEvent(_, _id, name, tags, language, place_id, country, privacy_type, activity_count, progress, type_partup, type_com_budget, type_org_budget, phase) =>
      Cypher(
        """MATCH (p:Partup {_id:'{_id}'})
           |SET p.name='{name}',
           |p.tags=[{tags}],
           |p.language='{language}',
           |p.place_id='{place_id}',
           |p.country='{country}',
           |p.privacy_type={privacy_type},
           |p.activity_count={activity_count},
           |p.progress={progress},
           |p.type_partup='{type_partup}',
           |p.type_com_budget={type_com_budget},
           |p.type_org_budget={type_org_budget},
           |p.phase='{phase}'
         """).on(("_id", _id), ("name", name), ("tags", tags), ("language", language), ("place_id", place_id), ("country", country), ("privacy_type", privacy_type), ("activity_count", activity_count), ("progress", progress), ("type_partup", type_partup), ("type_com_budget", type_com_budget), ("type_org_budget", type_org_budget), ("phase", phase))
        .execute()(conn)

    case PartupsChangedEvent(_, _id, name, tags, language, place_id, country, privacy_type, activity_count, progress, type_partup, type_com_budget, type_org_budget, phase) =>
      Cypher(
        """MATCH (p:Partup {_id:'{_id}'})
           |SET p.name='{name}',
           |p.tags=[{tags}],
           |p.language='{language}',
           |p.place_id='{place_id}',
           |p.country='{country}',
           |p.privacy_type={privacy_type},
           |p.activity_count={activity_count},
           |p.progress={progress},
           |p.type_partup='{type_partup}',
           |p.type_com_budget={type_com_budget},
           |p.type_org_budget={type_org_budget},
           |p.phase='{phase}'
         """).on(("_id", _id), ("name", name), ("tags", tags), ("language", language), ("place_id", place_id), ("country", country), ("privacy_type", privacy_type), ("activity_count", activity_count), ("progress", progress), ("type_partup", type_partup), ("type_com_budget", type_com_budget), ("type_org_budget", type_org_budget), ("phase", phase))
        .execute()(conn)

    case PartupsRemovedEvent(_, _id) =>
      Cypher(
        """MATCH (p:Partup {_id:'{_id}'})
           |DETACH DELETE p
         """).on(("_id", _id))
        .execute()(conn)

    //Tribes
    case TribesInsertedEvent(_, _id, name, privacy_type, admin_id) =>
      Cypher(
        """MATCH (u:User {_id:'{admin_id}'})
           |CREATE (t:Tribe {_id:'{_id}',
           |name:'{name}',
           |privacy_type:'{privacy_type}'}),
           |(u)-[:ADMIN_IN]->(t)
         """).on(("_id", _id), ("name", name), ("privacy_type", privacy_type), ("admin_id", admin_id))
        .execute()(conn)

    case TribesUpdatedEvent(_, _id, name, privacy_type) =>
      Cypher(
        """MATCH (t:Tribe {_id:'{_id}'})
           |SET t.name='{name}',
           |t.privacy_type='{privacy_type}'
         """).on(("_id", _id), ("name", name), ("privacy_type", privacy_type))
        .execute()(conn)

    case TribesChangedEvent(_, _id, name, privacy_type) =>
      Cypher(
        """MATCH (t:Tribe {_id:'{_id}'})
           |SET t.name='{name}',
           |t.privacy_type='{privacy_type}'
         """).on(("_id", _id), ("name", name), ("privacy_type", privacy_type))
        .execute()(conn)

    case TribesRemovedEvent(_, _id) =>
      Cypher(
        """MATCH (t:Tribe {_id:'{_id}'})
           |DETACH DELETE t
         """).on(("_id", _id))
        .execute()(conn)

    //Users
    case UsersInsertedEvent(_, _id, name, place_id, country, tags) =>
      Cypher(
        """MERGE (u:User {_id:'{_id}'})
           |ON CREATE SET u.name='{name}',
           |u.place_id='{place_id}',
           |u.country='{country}',
           |u.tags=[{tags}]
         """).on(("_id", _id), ("name", name), ("place_id", place_id), ("country", country), ("tags", tags))
        .execute()(conn)

    case UsersUpdatedEvent(_, _id, name, place_id, country, tags) =>
      Cypher(
        """MATCH (u:User {_id:'{_id}'})
           |SET u.name='{name}',
           |u.place_id='{place_id}',
           |u.country='{country}',
           |u.tags=[{tags}]
         """).on(("_id", _id), ("name", name), ("place_id", place_id), ("country", country), ("tags", tags))
        .execute()(conn)

    case UsersChangedEvent(_, _id, name, place_id, country, tags) =>
      Cypher(
        """MATCH (u:User {_id:'{_id}'})
           |SET u.name='{name}',
           |u.place_id='{place_id}',
           |u.country='{country}',
           |u.tags=[{tags}]
         """).on(("_id", _id), ("name", name), ("place_id", place_id), ("country", country), ("tags", tags))
        .execute()(conn)

    //Partners
    case PartnersInsertedEvent(_, _id, partup_id) =>
      Cypher(
        """MATCH (u:User {_id:'{_id}'),
           |(p:Partup {_id:'{partup_id}'})
           |OPTIONAL MATCH (u)-[r:SUPPORTER_IN]->(p)
           |CREATE (u)-[:PARTNER_IN {pageViews: r.pageViews, contributions:1}]->(p)
           |DELETE r
         """).on(("_id", _id), ("partup_id", partup_id))
        .execute()(conn)

    //Supporters
    case SupportersInsertedEvent(_, _id, partup_id) =>
      Cypher(
        """Match (u:User {_id:'{_id}'),
           |(p:Partup {_id:'{partup_id}'})
           |OPTIONAL MATCH (u)-[r:NON_SUPPORTER_IN]->(p)
           |DELETE r
           |CREATE (u)-[:SUPPORTER_IN]->(p)
         """).on(("_id", _id), ("partup_id", partup_id))
        .execute()(conn)

    case SupportersRemovedEvent(_, _id, partup_id) =>
      Cypher(
        """MATCH (u:User {_id:'{_id}'})-[r:SUPPORTER_IN]->(p:Partup {_id:'{partup_id}'})
           |DELETE r
           |CREATE (u)-[:NON_SUPPORTER_IN]->(p)
         """).on(("_id", _id), ("partup_id", partup_id))
        .execute()(conn)

    //Members
    case MembersAcceptedEvent(_, _id, tribe_id) =>
      Cypher(
        """MATCH (u:User {_id:'{_id}'}),
          |(t:Tribe {_id:'{tribe_id}'})
          |CREATE (u)-[:MEMBER_IN]->(t)
        """).on(("_id", _id), ("tribe_id", tribe_id))
        .execute()(conn)

    case MembersInsertedEvent(_, _id, tribe_id) =>
      Cypher(
        """MATCH (u:User {_id:'{_id}'}),
          |(t:Tribe {_id:'{tribe_id}'})
          |CREATE (u)-[:MEMBER_IN]->(t)
        """).on(("_id", _id), ("tribe_id", tribe_id))
        .execute()(conn)

    case MembersRemovedEvent(_, _id, tribe_id) =>
      Cypher(
        """MATCH (u:User {_id:'{_id'})-[r:MEMBER_IN]->(t:Tribe {_id:'{tribe_id}'})
          |DELETE r
          |CREATE (u)-[:NON_MEMBER_IN]->(t)
        """).on(("_id", _id), ("tribe_id", tribe_id))
        .execute()(conn)

    //Analytics
    case AnalyticsPageViewEvent(_, _id, partup_id) =>
      Cypher(
        """MATCH (u:User {_id:'{_id}'}-[r]->(p:Partup {partup_id:'{'partup_id}'}),
          |SET r.pageViews=r.pageViews+1
        """).on(("_id", _id), ("partup_id", partup_id))
        .execute()(conn)

    //Contributions
    case ContributionsInsertedEvent(_, _id, partup_id) =>
      Cypher(
        """MATCH (u:User {_id:'{_id}'})-[r]->(p:Partup {_id:'{partup_id}'})
          |SET r.contributions=r.contributions+1
        """).on(("_id", _id), ("partup_id", partup_id))
        .execute()(conn)

    //Comments
    case CommentsInsertedEvent(_, _id, partup_id) =>
      Cypher(
        """MATCH (u:User {_id:'{_id}'})-[r]->(p:Partner {_id:'{partup_id}'})
          |SET r.comments=r.comments+1
        """).on(("_id", _id), ("partup_id", partup_id))
        .execute()(conn)
  }

  override def receive = {
    case PartupCreatedEvent(_, name, id, _) =>
      Cypher("CREATE (n:partup{name:'{name}', id:'{id}'})")
        .on(("name", name), ("id", id))
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
