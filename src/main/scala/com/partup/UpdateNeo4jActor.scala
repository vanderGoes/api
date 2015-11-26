package com.partup

import akka.actor.Actor
import org.anormcypher.{Neo4jREST, _}

/**
  * Updates the graph database based on the events received from Meteor
  */
class UpdateNeo4jActor(conn: Neo4jREST) extends Actor {

  override def receive = {

    //NODES
    //Users
    case UsersInsertedEvent(_, _id, name, language, place_id, country, tags) =>
      val query_me_location = """MERGE (ci:City {_id: '{place_id}'})
                                |ON CREATE SET ci.name= '{city}'
                                |MERGE (co:Country {name: '{country}'})""".stripMargin
      val query_me_user = """MERGE (u:User {_id:'{_id}'})
          |SET u.name='{name}',
          |u.language='{language}',
          |u.tags=[{tags}]""".stripMargin
      val query_cu_location = """CREATE UNIQUE (u)-[:LOCATED_IN]->(ci),
                                |(ci)-[:LOCATED_IN]->(co)""".stripMargin

      val query = {
        if (place_id != null)
          query_me_location
      } + query_me_user + {
        if (place_id != null)
          query_cu_location
      }
      Cypher(query).on(("_id", _id), ("name", name), ("language", language), ("place_id", place_id), ("country", country), ("tags", tags))
        .execute()(conn)

    case UsersUpdatedEvent(_, _id, name, language, place_id, country, tags) =>
      val query_me_location = """MERGE (ci:City {_id: '{place_id}'})
                                |ON CREATE SET ci.name= '{city}'
                                |MERGE (co:Country {name: '{country}'})""".stripMargin
      val query_me_user = """MERGE (u:User {_id:'{_id}'})
          |SET u.name='{name}',
          |u.language='{language}',
          |u.tags=[{tags}]""".stripMargin
      val query_cu_location = """CREATE UNIQUE (u)-[:LOCATED_IN]->(ci),
                                |(ci)-[:LOCATED_IN]->(co)""".stripMargin

      val query = {
        if (place_id != null)
          query_me_location
      } + query_me_user + {
        if (place_id != null)
          query_cu_location
      }
      Cypher(query).on(("_id", _id), ("name", name), ("language", language), ("place_id", place_id), ("country", country), ("tags", tags))
        .execute()(conn)

    case UsersChangedEvent(_, _id, name, language, place_id, country, tags) =>
      val query_me_location = """MERGE (ci:City {_id: '{place_id}'})
                                |ON CREATE SET ci.name= '{city}'
                                |MERGE (co:Country {name: '{country}'})""".stripMargin
      val query_me_user = """MERGE (u:User {_id:'{_id}'})
          |SET u.name='{name}',
          |u.language='{language}',
          |u.tags=[{tags}]""".stripMargin
      val query_cu_location = """CREATE UNIQUE (u)-[:LOCATED_IN]->(ci),
                                |(ci)-[:LOCATED_IN]->(co)""".stripMargin

      val query = {
        if (place_id != null)
          query_me_location
      } + query_me_user + {
        if (place_id != null)
          query_cu_location
      }
      Cypher(query).on(("_id", _id), ("name", name), ("language", language), ("place_id", place_id), ("country", country), ("tags", tags))
        .execute()(conn)

    //Networks
    case TribesInsertedEvent(_, _id, name, privacy_type, admin_id, place_id, city, country) =>
      val query_me_user = "MERGE (u:User {_id:'{admin_id}'})"
      val query_me_location = """MERGE (ci:City {_id: '{place_id}'})
                                |ON CREATE SET ci.name= '{city}'
                                |MERGE (co:Country {name: '{country}'})""".stripMargin
      val query_me_network = """MERGE (n:Network {_id:'{_id}'})
                              |SET n.name:'{name}',
                              |n.privacy_type:'{privacy_type}'})
                              |CREATE UNIQUE (u)-[:MEMBER_OF {admin:true}]->(n)""".stripMargin
      val query_cu_location = """CREATE UNIQUE (n)-[:LOCATED_IN]->(ci),
                                |(ci)-[:LOCATED_IN]->(co)""".stripMargin

      val query = query_me_user + {
        if (place_id != null)
          query_me_location
      } + query_me_network + {
        if (place_id != null)
          query_cu_location
      }
      Cypher(query).on(("_id", _id), ("name", name), ("privacy_type", privacy_type), ("admin_id", admin_id), ("place_id", place_id), ("city", city), ("country", country))
        .execute()(conn)

    case TribesUpdatedEvent(_, _id, name, privacy_type, place_id, city, country) =>
      val query_me_location = """MERGE (ci:City {_id: '{place_id}'})
                                |ON CREATE SET ci.name= '{city}'
                                |MERGE (co:Country {name: '{country}'})""".stripMargin
      val query_me_network = """MERGE (n:Network {_id:'{_id}'})
                               |SET n.name:'{name}',
                               |n.privacy_type:'{privacy_type}'})""".stripMargin
      val query_cu_location = """CREATE UNIQUE (n)-[:LOCATED_IN]->(ci),
                                |(ci)-[:LOCATED_IN]->(co)""".stripMargin

      val query = {
        if (place_id != null)
          query_me_location
      } + query_me_network + {
        if (place_id != null)
          query_cu_location
      }
      Cypher(query).on(("_id", _id), ("name", name), ("privacy_type", privacy_type), ("place_id", place_id), ("city", city), ("country", country))
        .execute()(conn)

    case TribesChangedEvent(_, _id, name, privacy_type, place_id, city, country) =>
      val query_me_location = """MERGE (ci:City {_id: '{place_id}'})
                                |ON CREATE SET ci.name= '{city}'
                                |MERGE (co:Country {name: '{country}'})""".stripMargin
      val query_me_network = """MERGE (n:Network {_id:'{_id}'})
                               |SET n.name:'{name}',
                               |n.privacy_type:'{privacy_type}'})""".stripMargin
      val query_cu_location = """CREATE UNIQUE (n)-[:LOCATED_IN]->(ci),
                                |(ci)-[:LOCATED_IN]->(co)""".stripMargin

      val query = {
        if (place_id != null)
          query_me_location
      } + query_me_network + {
        if (place_id != null)
          query_cu_location
      }
      Cypher(query).on(("_id", _id), ("name", name), ("privacy_type", privacy_type), ("place_id", place_id), ("city", city), ("country", country))
        .execute()(conn)

    case TribesRemovedEvent(_, _id) =>
      Cypher(
        """MATCH (n:Network {_id:'{_id}'})
          |DETACH DELETE n
        """).on(("_id", _id))
        .execute()(conn)

    //Teams
    case PartupsInsertedEvent(_, creator_id, _id, name, tags, language, place_id, city, country, network_id, privacy_type, type_partup, phase) =>

      val query_me_user = "MERGE (u:User {_id:'{creator_id}'})"
      val query_me_network = "MERGE (n:Network {_id: '{network_id}'})"
      val query_me_location = """MERGE (ci:City {_id: '{place_id}'})
                                  |ON CREATE SET ci.name= '{city}'
                                  |MERGE (co:Country {name: '{country}'})""".stripMargin
      val query_me_team = """MERGE (t:Team {_id:'{_id}'})
                             |SET t.name='{name}',
                             |t.tags=[{tags}],
                             |t.language='{language}',
                             |t.privacy_type={privacy_type},
                             |t.type_partup='{type_partup}',
                             |t.phase='{phase}'
                             |CREATE UNIQUE (u)-[:PARTNER_IN {creator:true}]->(t)""".stripMargin
      val query_cu_network = "CREATE UNIQUE (t)-[:PART_OF]->(n)"
      val query_cu_location = """CREATE UNIQUE (t)-[:LOCATED_IN]->(ci),
            |(ci)-[:LOCATED_IN]->(co)""".stripMargin

    val query = query_me_user + {
      if (network_id != null)
        query_me_network
    } + {
      if (place_id != null)
        query_me_location
    } + query_me_team + {
      if (network_id != null)
        query_cu_network
    } + {
      if (place_id != null)
        query_cu_location
    }
      Cypher(query).on(("creator_id", creator_id), ("_id", _id), ("name", name), ("tags", tags), ("language", language), ("place_id", place_id), ("city", city), ("country", country), ("network_id", network_id), ("privacy_type", privacy_type), ("type_partup", type_partup), ("phase", phase))
        .execute()(conn)

    case PartupsUpdatedEvent(_, _id, name, tags, language, place_id, city, country, privacy_type, type_partup, phase) =>
      val query_me_location = """MERGE (ci:City {_id: '{place_id}'})
                                |ON CREATE SET ci.name= '{city}'
                                |MERGE (co:Country {name: '{country}'})""".stripMargin
      val query_me_team = """MERGE (t:Team {_id:'{_id}'})
                              |SET t.name='{name}',
                              |t.tags=[{tags}],
                              |t.language='{language}',
                              |t.privacy_type={privacy_type},
                              |t.type_partup='{type_partup}',
                              |t.phase='{phase}'""".stripMargin
      val query_cu_location = """CREATE UNIQUE (t)-[:LOCATED_IN]->(ci),
                                |(ci)-[:LOCATED_IN]->(co)""".stripMargin

      val query = {
        if (place_id != null)
          query_me_location
      } + query_me_team + {
        if (place_id != null)
          query_cu_location
      }
      Cypher(query).on(("_id", _id), ("name", name), ("tags", tags), ("language", language), ("place_id", place_id), ("city", city), ("country", country), ("privacy_type", privacy_type), ("type_partup", type_partup), ("phase", phase))
        .execute()(conn)

    case PartupsChangedEvent(_, _id, name, tags, language, place_id, city, country, privacy_type, type_partup, phase) =>
      val query_me_location = """MERGE (ci:City {_id: '{place_id}'})
                                |ON CREATE SET ci.name= '{city}'
                                |MERGE (co:Country {name: '{country}'})""".stripMargin
      val query_me_team ="""MERGE (t:Team {_id:'{_id}'})
                          |SET t.name='{name}',
                          |t.tags=[{tags}],
                          |t.language='{language}',
                          |t.privacy_type={privacy_type},
                          |t.type_partup='{type_partup}',
                          |t.phase='{phase}'""".stripMargin
      val query_cu_location = """CREATE UNIQUE (t)-[:LOCATED_IN]->(ci),
                                |(ci)-[:LOCATED_IN]->(co)""".stripMargin

      val query = {
        if (place_id != null)
          query_me_location
      } + query_me_team + {
        if (place_id != null)
          query_cu_location
      }
      Cypher(query).on(("_id", _id), ("name", name), ("tags", tags), ("language", language), ("place_id", place_id), ("city", city), ("country", country), ("privacy_type", privacy_type), ("type_partup", type_partup), ("phase", phase))
        .execute()(conn)

    case PartupsRemovedEvent(_, _id) =>
      Cypher(
        """MATCH (t:Team {_id:'{_id}'})
           |DETACH DELETE t
         """).on(("_id", _id))
        .execute()(conn)

    //EDGES
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
        """MATCH (u:User {_id:'{_id}'})-[r:PARTNER_IN]->(t:Team {_id:'{partup_id}'})
          |SET r.contributions=r.contributions+1
        """).on(("_id", _id), ("partup_id", partup_id))
        .execute()(conn)

    case ContributionsRemovedEvent(_, _id, partup_id) =>
      Cypher(
        """MATCH (u:User {_id:'{_id}'})-[r:PARTNER_IN]->(t:Team {_id:'{partup_id}'})
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
