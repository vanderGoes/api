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
    case UsersInsertedEvent(_, _id, language, deactivatedAt, place_id, city, country, tags, code_0, name_0, score_0, code_1, name_1, score_1) =>
      val query_me_location = "MERGE (ci:City {_id: '{place_id}'})\n" +
        "ON CREATE SET ci.name= '{city}'\n" +
        "MERGE (co:Country {name: '{country}'})\n"
      val query_me_user = "MERGE (u:User {_id:'{_id}'})\n" +
        "SET u.language='{language}'," +
        "u.maxContributions=0," +
        "u.maxComments=0," +
        "u.active=true\n"
      val query_me_strength = "MERGE (s0:Strength {code: '{code_0}'})\n" +
        "ON CREATE SET s0.name= '" + name_0 + "'\n" +
        "MERGE (s1:Strength {code: '" + code_1 + "'})\n" +
        "ON CREATE SET s1.name= '" + name_1 + "'\n"
      val tagsAsString = tags.map("'" + _ + "'").reduce((acc, it: String) => acc + "," + it)
      val query_set_tags = s"SET u.tags=[$tagsAsString]\n"
      val query_deactivated = "SET u.deactivatedAt={deactivatedAt}," +
        "u.active=false\n"
      val query_cu_location = "CREATE UNIQUE (u)-[:LIVES_IN]->(ci)," +
        "(ci)-[:LIVES_IN]->(co)"
      val query_cu_strength = "CREATE UNIQUE (u)-[r0:HOLDS]->(s0)\n" +
        "SET r0.score={score_0}\n" +
        "CREATE UNIQUE (u)-[r1:HOLDS]->(s1)\n" +
        "SET r1.score={score_1}"

      val query = {
        if (place_id != null)
          query_me_location
      } +
        query_me_user + {
        if (code_0 != 0)
          query_me_strength
      } + {
        if (tags != null)
          query_set_tags
      } + {
        if (deactivatedAt != null)
          query_deactivated
      } + {
        if (place_id != null)
          query_cu_location
      } + {
        if (code_0 != 0)
          query_cu_strength
      }
      Cypher(query).on(("_id", _id), ("deactivatedAt", deactivatedAt), ("place_id", place_id), ("country", country), ("tags", tags), ("code_0", code_0), ("name_0", name_0), ("score_0", score_0), ("code_1", code_1), ("name_1", name_1), ("score_1", score_1))
        .execute()(conn)

    case UsersUpdatedEvent(_, _id, language, deactivatedAt, place_id, city, country, tags, code_0, name_0, score_0, code_1, name_1, score_1) =>
      val query_me_location = "MERGE (ci:City {_id: '{place_id}'})\n" +
        "ON CREATE SET ci.name= '{city}'\n" +
        "MERGE (co:Country {name: '{country}'})\n"
      val query_me_user = "MERGE (u:User {_id:'{_id}'})\n" +
        "SET u.language='{language}'\n"
      val query_me_strength = "MERGE (s0:Strength {code: '{code_0}'})\n" +
        "ON CREATE SET s0.name= '{name_0}'\n" +
        "MERGE (s1:Strength {code: '{code_1}' })\n" +
        "ON CREATE SET s1.name= '{name_1}'\n"
      val tagsAsString = tags.map("'" + _ + "'").reduce((acc, it: String) => acc + "," + it)
      val query_set_tags = s"SET u.tags=[$tagsAsString]\n"
      val query_deactivated = "SET u.deactivatedAt={deactivatedAt}," +
        "u.active=false\n"
      val query_cu_location = "CREATE UNIQUE (u)-[:LIVES_IN]->(ci)," +
        "(ci)-[:LIVES_IN]->(co)\n"
      val query_cu_strength = "CREATE UNIQUE (u)-[r0:HOLDS]->(s0)\n" +
        "SET r0.score={score_0}\n" +
        "CREATE UNIQUE (u)-[r1:HOLDS]->(s1)\n" +
        "SET r1.score={score_1}"

      val query = {
        if (place_id != null)
          query_me_location
      } +
        query_me_user + {
        if (code_0 != 0)
          query_me_strength
      } + {
        if (tags != null)
          query_set_tags
      } + {
        if (deactivatedAt != null)
          query_deactivated
      } + {
        if (place_id != null)
          query_cu_location
      } + {
        if (code_0 != 0)
          query_cu_strength
      }
      Cypher(query).on(("_id", _id), ("language", language), ("place_id", place_id), ("country", country), ("tags", tags), ("code_0", code_0), ("name_0", name_0), ("score_0", score_0), ("code_1", code_1), ("name_1", name_1), ("score_1", score_1))
        .execute()(conn)

    case UsersChangedEvent(_, _id, language, deactivatedAt, place_id, city, country, tags, code_0, name_0, score_0, code_1, name_1, score_1) =>
      val query_me_location = "MERGE (ci:City {_id: '{place_id}'})\n" +
        "ON CREATE SET ci.name= '{city}'\n" +
        "MERGE (co:Country {name: '{country}'})\n"
      val query_me_user = "MERGE (u:User {_id:'{_id}'})\n" +
        "SET u.language='{language}'\n"
      val query_me_strength = "MERGE (s0:Strength {code: '" + code_0 + "'})\n" +
        "ON CREATE SET s0.name= '{name_0}'\n" +
        "MERGE (s1:Strength {code: '{code_1}'})\n" +
        "ON CREATE SET s1.name= '{name_1}'\n"
      val tagsAsString = tags.map("'" + _ + "'").reduce((acc, it: String) => acc + "," + it)
      val query_set_tags = s"SET u.tags=[$tagsAsString]\n"
      val query_deactivated = "SET u.deactivatedAt={deactivatedAt}," +
        "u.active=false\n"
      val query_cu_location = "CREATE UNIQUE (u)-[:LIVES_IN]->(ci)," +
        "(ci)-[:LIVES_IN]->(co)\n"
      val query_cu_strength = "CREATE UNIQUE (u)-[r0:HOLDS]->(s0)\n" +
        "SET r0.score={score_0}\n" +
        "CREATE UNIQUE (u)-[r1:HOLDS]->(s1)\n" +
        "SET r1.score={score_1}"

      val query = {
        if (place_id != null)
          query_me_location
      } +
        query_me_user + {
        if (code_0 != 0)
          query_me_strength
      } + {
        if (tags != null)
          query_set_tags
      } + {
        if (deactivatedAt != null)
          query_deactivated
      } + {
        if (place_id != null)
          query_cu_location
      } + {
        if (code_0 != 0)
          query_cu_strength
      }

      Cypher(query).on(("_id", _id), ("language", language), ("place_id", place_id), ("country", country), ("tags", tags), ("code_0", code_0), ("name_0", name_0), ("score_0", score_0), ("code_1", code_1), ("name_1", name_1), ("score_1", score_1))
        .execute()(conn)

    //Networks
    case TribesInsertedEvent(_, _id, name, privacy_type, admin_id, language, place_id, city, country, tags) =>
      val query_me_user = "MERGE (u:User {_id:'{admin_id}'})\n"
      val query_me_location = "MERGE (ci:City {_id: '{place_id}'})\n" +
                                "ON CREATE SET ci.name= '{city}'\n" +
                                "MERGE (co:Country {name: '{country}'})\n"
      val query_me_network = "MERGE (n:Network {_id:'{_id}'})\n" +
                              "SET n.name='{name}'," +
                              "n.language='{language}'," +
                              "n.privacy_type:{privacy_type}\n" +
                              "CREATE UNIQUE (u)-[:MEMBER_OF {admin:true}]->(n)\n"
      val tagsAsString = tags.map("'" + _ + "'").reduce((acc, it : String) => acc + "," + it)
      val query_set_tags = s"SET n.tags=[$tagsAsString]\n"
      val query_cu_location = "CREATE UNIQUE (n)-[:LOCATED_IN]->(ci)," +
                                "(ci)-[:LOCATED_IN]->(co)"

      val query = query_me_user + {
        if (place_id != null)
          query_me_location
      } + query_me_network + {
        if (tags != null)
          query_set_tags } + {
        if (place_id != null)
          query_cu_location }

      Cypher(query).on(("_id", _id), ("name", name), ("privacy_type", privacy_type), ("admin_id", admin_id), ("language", language), ("place_id", place_id), ("city", city), ("country", country), ("tags", tags))
        .execute()(conn)

    case TribesUpdatedEvent(_, _id, name, privacy_type, language, place_id, city, country, tags) =>
      val query_me_location = "MERGE (ci:City {_id: '{place_id}'})\n" +
                                "ON CREATE SET ci.name= '{city}'\n" +
                                "MERGE (co:Country {name: '{country}'})\n"
      val query_me_network = "MERGE (n:Network {_id:'{_id}'})\n" +
                               "SET n.name='{name}'," +
                               "n.language='{language}'," +
                               "n.privacy_type:{privacy_type}\n"
      val tagsAsString = tags.map("'" + _ + "'").reduce((acc, it : String) => acc + "," + it)
      val query_set_tags = s"SET n.tags=[$tagsAsString]\n"
      val query_cu_location = "CREATE UNIQUE (n)-[:LOCATED_IN]->(ci)," +
                                "(ci)-[:LOCATED_IN]->(co)"

      val query = {
        if (place_id != null)
          query_me_location
      } + query_me_network + {
        if (tags != null)
          query_set_tags } + {
        if (place_id != null)
          query_cu_location }

      Cypher(query).on(("_id", _id), ("name", name), ("privacy_type", privacy_type), ("language", language), ("place_id", place_id), ("city", city), ("country", country), ("tags", tags))
        .execute()(conn)

    case TribesChangedEvent(_, _id, name, privacy_type, language, place_id, city, country, tags) =>
      val query_me_location = "MERGE (ci:City {_id: '{place_id}'})\n" +
                                "ON CREATE SET ci.name= '{city}'\n" +
                                "MERGE (co:Country {name: '{country}'})\n"
      val query_me_network = "MERGE (n:Network {_id:'{_id}'})\n" +
                               "SET n.name='{name}'," +
                               "n.language='{language}'," +
                               "n.privacy_type:{privacy_type}\n"
      val tagsAsString = tags.map("'" + _ + "'").reduce((acc, it : String) => acc + "," + it)
      val query_set_tags = s"SET n.tags=[$tagsAsString] "
      val query_cu_location = "CREATE UNIQUE (n)-[:LOCATED_IN]->(ci)," +
                                "(ci)-[:LOCATED_IN]->(co) "

      val query = {
        if (place_id != null)
          query_me_location
      } + query_me_network + {
        if (tags != null)
          query_set_tags } + {
        if (place_id != null)
          query_cu_location }

      Cypher(query).on(("_id", _id), ("name", name), ("privacy_type", privacy_type), ("language", language), ("place_id", place_id), ("city", city), ("country", country), ("tags", tags))
        .execute()(conn)

    case TribesRemovedEvent(_, _id) =>
      Cypher(
        "MATCH (n:Network {_id:'{_id}'})\n" +
          "DETACH DELETE n"
        ).on(("_id", _id))
        .execute()(conn)

    //Teams
    case PartupsInsertedEvent(_, creator_id, _id, name, tags, language, place_id, city, country, network_id, privacy_type, type_partup, phase, activity_count, end_date, deactivatedAt) =>

      val query_me_user = "MERGE (u:User {_id:'{creator_id}'})\n"
      val query_me_network = "MERGE (n:Network {_id: '{network_id}'})\n"
      val query_me_location = "MERGE (ci:City {_id: '{place_id}'})\n" +
                                "ON CREATE SET ci.name= '{city}'\n" +
                                "MERGE (co:Country {name: '{country}'})\n"
      val query_me_team = "MERGE (t:Team {_id:'{_id}'})\n" +
                             "SET t.name='{name}'," +
                             "t.language='{language}'," +
                             "t.privacy_type={privacy_type}," +
                             "t.type='{type_partup}'," +
                             "t.phase='{phase}'," +
                             "t.activity_count={activity_count}," +
                             "t.partners=1," +
                             "t.end_date={end_date}," +
                             "t.active=true\n" +
                             "CREATE UNIQUE (u)-[:ACTIVE_IN {creator:true, comments:0, contributions:0, pageViews:0, participation:2.0, ratings:[], role:2.0}]->(t)\n"
      val tagsAsString = tags.map("'" + _ + "'").reduce((acc, it : String) => acc + "," + it)
      val query_set_tags = s"SET t.tags=[$tagsAsString]\n"
      val query_deactivated = "SET t.deactivatedAt={deactivatedAt}," +
                                "t.active=false\n"
      val query_cu_network = "CREATE UNIQUE (t)-[:PART_OF]->(n)\n"
      val query_cu_location = "CREATE UNIQUE (t)-[:LOCATED_IN]->(ci)," +
                                "(ci)-[:LOCATED_IN]->(co)"

    val query = query_me_user + {
      if (network_id != null)
        query_me_network } + {
      if (place_id != null)
        query_me_location } +
      query_me_team + {
      if (tags != null)
        query_set_tags } + {
      if (deactivatedAt != null)
        query_deactivated } + {
      if (network_id != null)
        query_cu_network } + {
      if (place_id != null)
        query_cu_location }

      Cypher(query).on(("creator_id", creator_id), ("_id", _id), ("name", name), ("tags", tags), ("language", language), ("place_id", place_id), ("city", city), ("country", country), ("network_id", network_id), ("privacy_type", privacy_type), ("type_partup", type_partup), ("phase", phase), ("activity_count", activity_count), ("end_date", end_date), ("deactivatedAt", deactivatedAt))
        .execute()(conn)

    case PartupsUpdatedEvent(_, _id, name, tags, language, place_id, city, country, privacy_type, type_partup, phase, activity_count, end_date, deactivatedAt) =>
      val query_me_location = "MERGE (ci:City {_id: '{place_id}'})\n" +
                                "ON CREATE SET ci.name= '{city}'\n" +
                                "MERGE (co:Country {name: '{country}'})\n"
      val query_me_team = "MERGE (t:Team {_id:'{_id}'})\n" +
                              "SET t.name='{name}'," +
                              "t.language='{language}'," +
                              "t.privacy_type={privacy_type}," +
                              "t.type='{type_partup}'," +
                              "t.phase='{phase}," +
                              "t.activity_count={activity_count}," +
                              "t.end_date={end_date}'/n"
      val tagsAsString = tags.map("'" + _ + "'").reduce((acc, it : String) => acc + "," + it)
      val query_set_tags = s"SET t.tags=[$tagsAsString]\n"
      val query_deactivated = "SET t.deactivatedAt={deactivatedAt}," +
                                "t.active=false\n"
      val query_cu_location = "CREATE UNIQUE (t)-[:LOCATED_IN]->(ci)," +
                                "(ci)-[:LOCATED_IN]->(co)"

      val query = {
        if (place_id != null)
          query_me_location } +
        query_me_team + {
        if (tags != null)
          query_set_tags } + {
        if (deactivatedAt != null)
          query_deactivated } + {
        if (place_id != null)
          query_cu_location }

      Cypher(query).on(("_id", _id), ("name", name), ("tags", tags), ("language", language), ("place_id", place_id), ("city", city), ("country", country), ("privacy_type", privacy_type), ("type_partup", type_partup), ("phase", phase), ("activity_count", activity_count), ("end_date", end_date), ("deactivatedAt", deactivatedAt))
        .execute()(conn)

    case PartupsChangedEvent(_, _id, name, tags, language, place_id, city, country, privacy_type, type_partup, phase, activity_count, end_date, deactivatedAt) =>
      val query_me_location = "MERGE (ci:City {_id: '{place_id}'})\n" +
                                "ON CREATE SET ci.name= '{city}'\n" +
                                "MERGE (co:Country {name: '{country}'})\n"
      val query_me_team ="MERGE (t:Team {_id:'{_id}'})\n" +
                            "SET t.name='{name}'," +
                            "t.language='{language}'," +
                            "t.privacy_type={privacy_type}," +
                            "t.type='{type_partup}'," +
                            "t.phase='{phase}," +
                            "t.activity_count={activity_count}," +
                            "t.end_date={end_date}'\n"
      val tagsAsString = tags.map("'" + _ + "'").reduce((acc, it : String) => acc + "," + it)
      val query_set_tags = s"SET t.tags=[$tagsAsString]\n"
      val query_deactivated = "SET t.deactivatedAt={deactivatedAt}," +
                                "t.active=false\n"
      val query_cu_location = "CREATE UNIQUE (t)-[:LOCATED_IN]->(ci)," +
                                "(ci)-[:LOCATED_IN]->(co)"

      val query = {
        if (place_id != null)
          query_me_location } +
        query_me_team + {
        if (tags != null)
          query_set_tags } + {
        if (deactivatedAt != null)
          query_deactivated } + {
        if (place_id != null)
          query_cu_location }

      Cypher(query).on(("_id", _id), ("name", name), ("tags", tags), ("language", language), ("place_id", place_id), ("city", city), ("country", country), ("privacy_type", privacy_type), ("type_partup", type_partup), ("phase", phase), ("activity_count", activity_count), ("end_date", end_date), ("deactivatedAt", deactivatedAt))
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
        "MATCH (u:User {_id:'{_id}')," +
           "(t:Team {_id:'{partup_id}'})\n" +
           "CREATE UNIQUE (u)-[r:ACTIVE_IN]->(t)\n" +
           "SET r.contributions=1," +
           "r.role=1.5," +
           "t.partners=t.partners+1"
         ).on(("_id", _id), ("partup_id", partup_id))
        .execute()(conn)

    //Supporters
    case SupportersInsertedEvent(_, _id, partup_id) =>
      Cypher(
        "MERGE (u:User {_id:'{_id}')," +
           "(t:Team {_id:'{partup_id}'})\n" +
           "CREATE UNIQUE (u)-[r:ACTIVE_IN]->(t)\n" +
           "SET r.role=1.0"
         ).on(("_id", _id), ("partup_id", partup_id))
        .execute()(conn)

    case SupportersRemovedEvent(_, _id, partup_id) =>
      Cypher(
        "MATCH (u:User {_id:'{_id}'})-[r:ACTIVE_IN {role:1.0}]->(t:Team {_id:'{partup_id}'})\n" +
           "DELETE r"
         ).on(("_id", _id), ("partup_id", partup_id))
        .execute()(conn)

    //Members
    case MembersAcceptedEvent(_, _id, tribe_id) =>
      Cypher(
        "MERGE (u:User {_id:'{_id}'})," +
          "(n:Network {_id:'{tribe_id}'})\n" +
          "CREATE UNIQUE (u)-[:MEMBER_OF]->(t)"
        ).on(("_id", _id), ("tribe_id", tribe_id))
        .execute()(conn)

    case MembersInsertedEvent(_, _id, tribe_id) =>
      Cypher(
        "MERGE (u:User {_id:'{_id}'})," +
          "(n:Network {_id:'{tribe_id}'})\n" +
          "CREATE UNIQUE (u)-[:MEMBER_OF]->(n)"
        ).on(("_id", _id), ("tribe_id", tribe_id))
        .execute()(conn)

    case MembersRemovedEvent(_, _id, tribe_id) =>
      Cypher(
        "MATCH (u:User {_id:'{_id'})-[r:MEMBER_OF]->(n:Network {_id:'{tribe_id}'})\n" +
          "DELETE r"
        ).on(("_id", _id), ("tribe_id", tribe_id))
        .execute()(conn)

    //Analytics
    case AnalyticsPageViewEvent(_, _id, partup_id) =>
      Cypher(
        "MATCH (u:User {_id:'{_id}'}-[r:ACTIVE_IN]->(t:Team {_id:'{'partup_id}'})\n" +
          "SET r.pageViews=r.pageViews+1"
        ).on(("_id", _id), ("partup_id", partup_id))
        .execute()(conn)

    //Contributions
    case ContributionsInsertedEvent(_, _id, partup_id) =>
      Cypher(
        "MATCH (u:User {_id:'{_id}'})-[r:ACTIVE_IN]->(t:Team {_id:'{partup_id}'})\n" +
          "SET r.contributions=r.contributions+1"
        ).on(("_id", _id), ("partup_id", partup_id))
        .execute()(conn)

    case ContributionsRemovedEvent(_, _id, partup_id) =>
      Cypher(
        "MATCH (u:User {_id:'{_id}'})-[r:ACTIVE_IN]->(t:Team {_id:'{partup_id}'})\n" +
          "SET r.contributions=r.contributions-1"
        ).on(("_id", _id), ("partup_id", partup_id))
        .execute()(conn)

    //Comments
    case CommentsInsertedEvent(_, _id, partup_id) =>
      Cypher(
        "MATCH (u:User {_id:'{_id}'})-[r:ACTIVE_IN]->(t:Team {_id:'{partup_id}'})\n" +
          "SET r.comments=r.comments+1"
        ).on(("_id", _id), ("partup_id", partup_id))
        .execute()(conn)

    case CommentsRemovedEvent(_, _id, partup_id) =>
      Cypher(
        "MATCH (u:User {_id:'{_id}'})-[r:ACTIVE_IN]->(t:Team {_id:'{partup_id}'})\n" +
          "SET r.comments=r.comments-1"
        ).on(("_id", _id), ("partup_id", partup_id))
        .execute()(conn)

    //Ratings
    case RatingsInsertedEvent(_, _, user_id, partup_id, rating) =>
      Cypher(
        "MATCH (u:User {_id:'{_id}'})-[r:ACTIVE_IN]->(t:Team {_id:'{partup_id}'})\n" +
          "SET r.ratings=r.ratings+[{rating}]"
        ).on(("user_id", user_id), ("partup_id", partup_id), ("rating", rating))
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
