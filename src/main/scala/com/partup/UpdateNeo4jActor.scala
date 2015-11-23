package com.partup

import akka.actor.Actor
import org.anormcypher.{Neo4jREST, _}

/**
  * Updates the graph database based on the events received from Meteor
  */
class UpdateNeo4jActor(conn: Neo4jREST) extends Actor {

  override def receive = {
    //Teams
    case PartupsInsertedEvent(_, creator_id, _id, name, tags, language, place_id, city, country, network_id, privacy_type, activity_count, progress, type_partup, type_com_budget, type_org_budget, phase) =>
      if (network_id != null) {
        Cypher(
          """MERGE (u:User {_id:'{creator_id}'})
            |MERGE (n:Network {_id: '{network_id}'})
            |MERGE (ci:City {_id: '{place_id}'})
            |ON CREATE SET ci.name: '{city}'
            |MERGE (co:Country {name: '{country}'})
            |MERGE (t:Team {_id:'{_id}'})
            |SET t.name:'{name}',
            |t.tags:[{tags}],
            |t.language:'{language}',
            |t.privacy_type:{privacy_type},
            |t.activity_count:{activity_count},
            |t.progress:{progress},
            |t.type_partup:'{type_partup}',
            |t.type_com_budget:{type_com_budget},
            |t.type_org_budget:{type_org_budget},
            |t.phase:'{phase}'
            |CREATE UNIQUE (u)-[:CREATOR_OF]->(t),
            |(t)-[:PART_OF]->(n),
            |(t)-[:LOCATED_IN]->(ci),
            |(ci)-[:LOCATED_IN]->(co)
          """).on(("creator_id", creator_id), ("_id", _id), ("name", name), ("tags", tags), ("language", language), ("place_id", place_id), ("city", city), ("country", country), ("network_id", network_id), ("privacy_type", privacy_type), ("activity_count", activity_count), ("progress", progress), ("type_partup", type_partup), ("type_com_budget", type_com_budget), ("type_org_budget", type_org_budget), ("phase", phase))
          .execute()(conn)
      }else{
        Cypher(
          """MERGE (u:User {_id:'{creator_id}'})
            |MERGE (ci:City {_id: '{place_id}'})
            |ON CREATE SET ci.name: '{city}'
            |MERGE (co:Country {name: '{country}'})
            |MERGE (t:Team {_id:'{_id}'})
            |SET t.name:'{name}',
            |t.tags:[{tags}],
            |t.language:'{language}',
            |t.privacy_type:{privacy_type},
            |t.activity_count:{activity_count},
            |t.progress:{progress},
            |t.type_partup:'{type_partup}',
            |t.type_com_budget:{type_com_budget},
            |t.type_org_budget:{type_org_budget},
            |t.phase:'{phase}'
            |CREATE UNIQUE (u)-[:CREATOR_OF]->(t),
            |(t)-[:LOCATED_IN]->(ci),
            |(ci)-[:LOCATED_IN]->(co)
          """).on(("creator_id", creator_id), ("_id", _id), ("name", name), ("tags", tags), ("language", language), ("place_id", place_id), ("country", country), ("privacy_type", privacy_type), ("activity_count", activity_count), ("progress", progress), ("type_partup", type_partup), ("type_com_budget", type_com_budget), ("type_org_budget", type_org_budget), ("phase", phase))
          .execute()(conn)
      }

    case PartupsUpdatedEvent(_, _id, name, tags, language, place_id, city, country, privacy_type, activity_count, progress, type_partup, type_com_budget, type_org_budget, phase) =>
      Cypher(
        """MERGE (ci:City {_id: '{place_id}'})
          |ON CREATE SET ci.name: '{city}'
          |MERGE (co:Country {name: '{country}'})
          |MERGE (t:Team {_id:'{_id}'})
          |SET t.name='{name}',
          |t.tags=[{tags}],
          |t.language='{language}',
          |t.place_id='{place_id}',
          |t.country='{country}',
          |t.privacy_type={privacy_type},
          |t.activity_count={activity_count},
          |t.progress={progress},
          |t.type_partup='{type_partup}',
          |t.type_com_budget={type_com_budget},
          |t.type_org_budget={type_org_budget},
          |t.phase='{phase}'
          |CREATE UNIQUE (t)-[:LOCATED_IN]->(ci),
          |(ci)-[:LOCATED_IN]->(co)
        """).on(("_id", _id), ("name", name), ("tags", tags), ("language", language), ("place_id", place_id), ("city", city), ("country", country), ("privacy_type", privacy_type), ("activity_count", activity_count), ("progress", progress), ("type_partup", type_partup), ("type_com_budget", type_com_budget), ("type_org_budget", type_org_budget), ("phase", phase))
        .execute()(conn)

    case PartupsChangedEvent(_, _id, name, tags, language, place_id, city, country, privacy_type, activity_count, progress, type_partup, type_com_budget, type_org_budget, phase) =>
      Cypher(
        """MERGE (ci:City {_id: '{place_id}'})
          |ON CREATE SET ci.name: '{city}'
          |MERGE (co:Country {name: '{country}'})
          |MERGE (t:Team {_id:'{_id}'})
          |SET t.name='{name}',
          |t.tags=[{tags}],
          |t.language='{language}',
          |t.place_id='{place_id}',
          |t.country='{country}',
          |t.privacy_type={privacy_type},
          |t.activity_count={activity_count},
          |t.progress={progress},
          |t.type_partup='{type_partup}',
          |t.type_com_budget={type_com_budget},
          |t.type_org_budget={type_org_budget},
          |t.phase='{phase}'
          |CREATE UNIQUE (t)-[:LOCATED_IN]->(ci),
          |(ci)-[:LOCATED_IN]->(co)
        """).on(("_id", _id), ("name", name), ("tags", tags), ("language", language), ("place_id", place_id), ("city", city), ("country", country), ("privacy_type", privacy_type), ("activity_count", activity_count), ("progress", progress), ("type_partup", type_partup), ("type_com_budget", type_com_budget), ("type_org_budget", type_org_budget), ("phase", phase))
        .execute()(conn)

    case PartupsRemovedEvent(_, _id) =>
      Cypher(
        """MATCH (t:Team {_id:'{_id}'})
           |DETACH DELETE t
         """).on(("_id", _id))
        .execute()(conn)

    //Networks
    case TribesInsertedEvent(_, _id, name, privacy_type, admin_id, place_id, city, country) =>
      Cypher(
        """MERGE (u:User {_id:'{admin_id}'})
          |MERGE (ci:City {_id: '{place_id}'})
          |ON CREATE SET ci.name: '{city}'
          |MERGE (co:Country {name: '{country}'})
          |MERGE (n:Network {_id:'{_id}'})
          |SET n.name:'{name}',
          |n.privacy_type:'{privacy_type}'}),
          |CREATE UNIQUE (u)-[:ADMIN_IN]->(n),
          |(n)-[:LOCATED_IN]->(ci),
          |(ci)-[:LOCATED_IN]->(co)
        """).on(("_id", _id), ("name", name), ("privacy_type", privacy_type), ("admin_id", admin_id), ("place_id", place_id), ("city", city), ("country", country))
        .execute()(conn)

    case TribesUpdatedEvent(_, _id, name, privacy_type, place_id, city, country) =>
      Cypher(
        """MERGE (ci:City {_id: '{place_id}'})
          |ON CREATE SET ci.name: '{city}'
          |MERGE (co:Country {name: '{country}'})
          |MERGE (n:Network {_id:'{_id}'})
          |SET n.name='{name}',
          |n.privacy_type='{privacy_type}'
          |CREATE UNIQUE (n)-[:LOCATED_IN]->(ci),
          |(ci)-[:LOCATED_IN]->(co)
        """).on(("_id", _id), ("name", name), ("privacy_type", privacy_type), ("place_id", place_id), ("city", city), ("country", country))
        .execute()(conn)

    case TribesChangedEvent(_, _id, name, privacy_type, place_id, city, country) =>
      Cypher(
        """MERGE (ci:City {_id: '{place_id}'})
          |ON CREATE SET ci.name: '{city}'
          |MERGE (co:Country {name: '{country}'})
          |MERGE (n:Network {_id:'{_id}'})
          |SET n.name='{name}',
          |n.privacy_type='{privacy_type}'
          |CREATE UNIQUE (n)-[:LOCATED_IN]->(ci),
          |(ci)-[:LOCATED_IN]->(co)
        """).on(("_id", _id), ("name", name), ("privacy_type", privacy_type), ("place_id", place_id), ("city", city), ("country", country))
      .execute()(conn)

    case TribesRemovedEvent(_, _id) =>
      Cypher(
        """MATCH (n:Network {_id:'{_id}'})
           |DETACH DELETE n
         """).on(("_id", _id))
        .execute()(conn)

    //Users
    case UsersInsertedEvent(_, _id, name, place_id, country, tags) =>
      Cypher(
        """MERGE (ci:City {_id: '{place_id}'})
           |ON CREATE SET ci.name: '{city}'
           |MERGE (co:Country {name: '{country}'})
           |MERGE (u:User {_id:'{_id}'})
           |SET u.name='{name}',
           |u.tags=[{tags}]
           |CREATE UNIQUE (u)-[:LOCATED_IN]->(ci),
           |(ci)-[:LOCATED_IN]->(co)
         """).on(("_id", _id), ("name", name), ("place_id", place_id), ("country", country), ("tags", tags))
        .execute()(conn)

    case UsersUpdatedEvent(_, _id, name, place_id, country, tags) =>
      Cypher(
        """MERGE (ci:City {_id: '{place_id}'})
           |ON CREATE SET ci.name: '{city}'
           |MERGE (co:Country {name: '{country}'})
           |MERGE (u:User {_id:'{_id}'})
           |SET u.name='{name}',
           |u.tags=[{tags}]
           |CREATE UNIQUE (u)-[:LOCATED_IN]->(ci),
           |(ci)-[:LOCATED_IN]->(co)
         """).on(("_id", _id), ("name", name), ("place_id", place_id), ("country", country), ("tags", tags))
        .execute()(conn)

    case UsersChangedEvent(_, _id, name, place_id, country, tags) =>
      Cypher(
        """MERGE (ci:City {_id: '{place_id}'})
           |ON CREATE SET ci.name: '{city}'
           |MERGE (co:Country {name: '{country}'})
           |MERGE (u:User {_id:'{_id}'})
           |SET u.name='{name}',
           |u.tags=[{tags}]
           |CREATE UNIQUE (u)-[:LOCATED_IN]->(ci),
           |(ci)-[:LOCATED_IN]->(co)
         """).on(("_id", _id), ("name", name), ("place_id", place_id), ("country", country), ("tags", tags))
        .execute()(conn)

    //Partners
    case PartnersInsertedEvent(_, _id, partup_id) =>
      Cypher(
        """MATCH (u:User {_id:'{_id}'),
           |(t:Team {_id:'{partup_id}'})
           |OPTIONAL MATCH (u)-[r:SUPPORTER_IN]->(t)
           |CREATE (u)-[:PARTNER_IN {pageViews: r.pageViews, contributions:1}]->(t)
           |DELETE r
         """).on(("_id", _id), ("partup_id", partup_id))
        .execute()(conn)

    //Supporters
    case SupportersInsertedEvent(_, _id, partup_id) =>
      Cypher(
        """Match (u:User {_id:'{_id}'),
           |(t:Team {_id:'{partup_id}'})
           |OPTIONAL MATCH (u)-[r:NON_SUPPORTER_IN]->(t)
           |DELETE r
           |CREATE (u)-[:SUPPORTER_IN]->(t)
         """).on(("_id", _id), ("partup_id", partup_id))
        .execute()(conn)

    case SupportersRemovedEvent(_, _id, partup_id) =>
      Cypher(
        """MATCH (u:User {_id:'{_id}'})-[r:SUPPORTER_IN]->(t:Team {_id:'{partup_id}'})
           |DELETE r
           |CREATE (u)-[:NON_SUPPORTER_IN]->(t)
         """).on(("_id", _id), ("partup_id", partup_id))
        .execute()(conn)

    //Members
    case MembersAcceptedEvent(_, _id, tribe_id) =>
      Cypher(
        """MATCH (u:User {_id:'{_id}'}),
          |(n:Network {_id:'{tribe_id}'})
          |CREATE (u)-[:MEMBER_IN]->(t)
        """).on(("_id", _id), ("tribe_id", tribe_id))
        .execute()(conn)

    case MembersInsertedEvent(_, _id, tribe_id) =>
      Cypher(
        """MATCH (u:User {_id:'{_id}'}),
          |(n:Network {_id:'{tribe_id}'})
          |CREATE (u)-[:MEMBER_IN]->(n)
        """).on(("_id", _id), ("tribe_id", tribe_id))
        .execute()(conn)

    case MembersRemovedEvent(_, _id, tribe_id) =>
      Cypher(
        """MATCH (u:User {_id:'{_id'})-[r]->(n:Network {_id:'{tribe_id}'})
          |DELETE r
          |CREATE (u)-[:NON_MEMBER_IN]->(n)
        """).on(("_id", _id), ("tribe_id", tribe_id))
        .execute()(conn)

    //Analytics
    case AnalyticsPageViewEvent(_, _id, partup_id) =>
      Cypher(
        """MATCH (u:User {_id:'{_id}'}-[r]->(t:Team {_id:'{'partup_id}'}),
          |SET r.pageViews=r.pageViews+1
        """).on(("_id", _id), ("partup_id", partup_id))
        .execute()(conn)

    //Contributions
    case ContributionsInsertedEvent(_, _id, partup_id) =>
      Cypher(
        """MATCH (u:User {_id:'{_id}'})-[r]->(t:Team {_id:'{partup_id}'})
          |SET r.contributions=r.contributions+1
        """).on(("_id", _id), ("partup_id", partup_id))
        .execute()(conn)

    case ContributionsRemovedEvent(_, _id, partup_id) =>
      Cypher(
        """MATCH (u:User {_id:'{_id}'})-[r]->(t:Team {_id:'{partup_id}'})
          |SET r.contributions=r.contributions-1
        """).on(("_id", _id), ("partup_id", partup_id))
        .execute()(conn)

    //Comments
    case CommentsInsertedEvent(_, _id, partup_id) =>
      Cypher(
        """MATCH (u:User {_id:'{_id}'})-[r]->(t:Team {_id:'{partup_id}'})
          |SET r.comments=r.comments+1
        """).on(("_id", _id), ("partup_id", partup_id))
        .execute()(conn)

    //Ratings
    case RatingsInsertedEvent(_, _, user_id, partup_id, rating) =>
      Cypher(
        """MATCH (u:User {_id:'{user_id}'})-[r:PARTNER_IN]->(t:Team {_id:'{partup_id}'})
          |SET r.ratings=r.ratings+['{rating}']
        """).on(("user_id", user_id), ("partup_id", partup_id), ("rating", rating))
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
