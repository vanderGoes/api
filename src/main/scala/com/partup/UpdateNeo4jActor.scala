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
    case UsersInsertedEvent(_, _id, name, email, language, deactivatedAt, place_id, city, country, tags) =>
      val query_me_location = "MERGE (ci:City {_id: '{place_id}'}) " +
                                "ON CREATE SET ci.name= '{city}' " +
                                "MERGE (co:Country {name: '{country}'}) "
      val query_me_user = "MERGE (u:User {_id:'{_id}'}) " +
                              "SET u.name='{name}', " +
                              "u.email='{email}', " +
                              "u.language='{language}'," +
                              "u.maxContributions=0, " +
                              "u.maxComments=0, " +
                              "u.active=true "
      val tagsAsString = tags.map("'" + _ + "'").reduce((acc, it : String) => acc + "," + it)
      val query_set_tags = s"SET u.tags=[$tagsAsString] "
      val query_deactivated = "SET u.deactivatedAt={deactivatedAt}, " +
                                "u.active=false "
      val query_cu_location = "CREATE UNIQUE (u)-[:LIVES_IN]->(ci), " +
                                "(ci)-[:LIVES_IN]->(co)"

      val query = {
          if (place_id != null)
            query_me_location } +
          query_me_user + {
          if (tags!=null)
            query_set_tags } + {
          if (deactivatedAt != null)
            query_deactivated } + {
          if (place_id != null)
            query_cu_location
      }
      Cypher(query).on(("_id", _id), ("name", name), ("language", language), ("deactivatedAt", deactivatedAt), ("place_id", place_id), ("country", country), ("tags", tags))
        .execute()(conn)

    case UsersUpdatedEvent(_, _id, name, email, language, deactivatedAt, place_id, city, country, tags) =>
      val query_me_location = "MERGE (ci:City {_id: '{place_id}'}) " +
                                "ON CREATE SET ci.name= '{city}' " +
                                "MERGE (co:Country {name: '{country}'}) "
      val query_me_user = "MERGE (u:User {_id:'{_id}'}) " +
                              "SET u.name='{name}', " +
                              "u.email='email', " +
                              "u.language='{language}' "
      val tagsAsString = tags.map("'" + _ + "'").reduce((acc, it : String) => acc + "," + it)
      val query_set_tags = s"SET u.tags=[$tagsAsString] "
      val query_deactivated = "SET u.deactivatedAt={deactivatedAt}, " +
                                "u.active=false "
      val query_cu_location = "CREATE UNIQUE (u)-[:LIVES_IN]->(ci), " +
                                "(ci)-[:LIVES_IN]->(co) "

      val query = {
        if (place_id != null)
          query_me_location } +
        query_me_user + {
        if (tags!=null)
          query_set_tags } + {
        if (deactivatedAt != null)
          query_deactivated } + {
        if (place_id != null)
          query_cu_location
      }
      Cypher(query).on(("_id", _id), ("name", name), ("email", email), ("language", language), ("place_id", place_id), ("country", country), ("tags", tags))
        .execute()(conn)

    case UsersChangedEvent(_, _id, name, email, language, deactivatedAt, place_id, city, country, tags) =>
      val query_me_location = "MERGE (ci:City {_id: '{place_id}'}) " +
                                "ON CREATE SET ci.name= '{city}' " +
                                "MERGE (co:Country {name: '{country}'}) "
      val query_me_user = "MERGE (u:User {_id:'{_id}'}) " +
                              "SET u.name='{name}', " +
                              "u.email='{email}', " +
                              "u.language='{language}' "
      val tagsAsString = tags.map("'" + _ + "'").reduce((acc, it : String) => acc + "," + it)
      val query_set_tags = s"SET u.tags=[$tagsAsString] "
      val query_deactivated = "SET u.deactivatedAt={deactivatedAt}, " +
                                "u.active=false "
      val query_cu_location = "CREATE UNIQUE (u)-[:LIVES_IN]->(ci), " +
                                "(ci)-[:LIVES_IN]->(co) "

      val query = {
        if (place_id != null)
          query_me_location } +
        query_me_user + {
        if (tags!=null)
          query_set_tags } + {
        if (deactivatedAt != null)
          query_deactivated } + {
        if (place_id != null)
          query_cu_location }

      Cypher(query).on(("_id", _id), ("name", name), ("email", email), ("language", language), ("place_id", place_id), ("country", country), ("tags", tags))
        .execute()(conn)

    //Networks
    case TribesInsertedEvent(_, _id, name, privacy_type, admin_id, language, place_id, city, country, tags) =>
      val query_me_user = "MERGE (u:User {_id:'{admin_id}'}) "
      val query_me_location = "MERGE (ci:City {_id: '{place_id}'}) " +
                                "ON CREATE SET ci.name= '{city}' " +
                                "MERGE (co:Country {name: '{country}'}) "
      val query_me_network = "MERGE (n:Network {_id:'{_id}'}) " +
                              "SET n.name='{name}', " +
                              "n.language='{language}', " +
                              "n.privacy_type:{privacy_type} " +
                              "CREATE UNIQUE (u)-[:MEMBER_OF {admin:true}]->(n) "
      val tagsAsString = tags.map("'" + _ + "'").reduce((acc, it : String) => acc + "," + it)
      val query_set_tags = s"SET n.tags=[$tagsAsString] "
      val query_cu_location = "CREATE UNIQUE (n)-[:LOCATED_IN]->(ci), " +
                                "(ci)-[:LOCATED_IN]->(co) "

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
      val query_me_location = "MERGE (ci:City {_id: '{place_id}'}) " +
                                "ON CREATE SET ci.name= '{city}' " +
                                "MERGE (co:Country {name: '{country}'}) "
      val query_me_network = "MERGE (n:Network {_id:'{_id}'}) " +
                               "SET n.name='{name}', " +
                               "n.language='{language}', " +
                               "n.privacy_type:{privacy_type} "
      val tagsAsString = tags.map("'" + _ + "'").reduce((acc, it : String) => acc + "," + it)
      val query_set_tags = s"SET n.tags=[$tagsAsString] "
      val query_cu_location = "CREATE UNIQUE (n)-[:LOCATED_IN]->(ci), " +
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

    case TribesChangedEvent(_, _id, name, privacy_type, language, place_id, city, country, tags) =>
      val query_me_location = "MERGE (ci:City {_id: '{place_id}'}) " +
                                "ON CREATE SET ci.name= '{city}' " +
                                "MERGE (co:Country {name: '{country}'}) "
      val query_me_network = "MERGE (n:Network {_id:'{_id}'}) " +
                               "SET n.name='{name}', " +
                               "n.language='{language}', " +
                               "n.privacy_type:{privacy_type} "
      val tagsAsString = tags.map("'" + _ + "'").reduce((acc, it : String) => acc + "," + it)
      val query_set_tags = s"SET n.tags=[$tagsAsString] "
      val query_cu_location = "CREATE UNIQUE (n)-[:LOCATED_IN]->(ci), " +
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
        "MATCH (n:Network {_id:'{_id}'}) " +
          "DETACH DELETE n"
        ).on(("_id", _id))
        .execute()(conn)

    //Teams
    case PartupsInsertedEvent(_, creator_id, _id, name, tags, purpose, language, place_id, city, country, network_id, privacy_type, type_partup, phase, activity_count, end_date, deactivatedAt) =>

      val query_me_user = "MERGE (u:User {_id:'{creator_id}'}) "
      val query_me_network = "MERGE (n:Network {_id: '{network_id}'}) "
      val query_me_location = "MERGE (ci:City {_id: '{place_id}'}) " +
                                "ON CREATE SET ci.name= '{city}' " +
                                "MERGE (co:Country {name: '{country}'}) "
      val query_me_team = "MERGE (t:Team {_id:'{_id}'}) " +
                             "SET t.name='{name}', " +
                             "t.language='{language}', " +
                             "t.privacy_type={privacy_type}, " +
                             "t.type='{type_partup}', " +
                             "t.phase='{phase}', " +
                             "t.activity_count={activity_count}, " +
                             "t.partners=1, " +
                             "t.end_date={end_date}, " +
                             "t.active=true " +
                             "CREATE UNIQUE (u)-[:ACTIVE_IN {creator:true, comments:0, contributions:0, pageViews:0, participation:2.0, ratings:[], weight:2.0}]->(t) "
      val tagsAsString = tags.map("'" + _ + "'").reduce((acc, it : String) => acc + "," + it)
      val query_set_tags = s"SET t.tags=[$tagsAsString] "
      val query_deactivated = "SET t.deactivatedAt={deactivatedAt}, " +
                                "t.active=false "
      val query_cu_network = "CREATE UNIQUE (t)-[:PART_OF]->(n) "
      val query_cu_location = "CREATE UNIQUE (t)-[:LOCATED_IN]->(ci), " +
                                "(ci)-[:LOCATED_IN]->(co) "

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

    case PartupsUpdatedEvent(_, _id, name, tags, purpose, language, place_id, city, country, privacy_type, type_partup, phase, activity_count, end_date, deactivatedAt) =>
      val query_me_location = "MERGE (ci:City {_id: '{place_id}'}) " +
                                "ON CREATE SET ci.name= '{city}' " +
                                "MERGE (co:Country {name: '{country}'}) "
      val query_me_team = "MERGE (t:Team {_id:'{_id}'}) " +
                              "SET t.name='{name}', " +
                              "t.language='{language}', " +
                              "t.privacy_type={privacy_type}, " +
                              "t.type='{type_partup}', " +
                              "t.phase='{phase}, " +
                              "t.activity_count={activity_count}, " +
                              "t.end_date={end_date}' "
      val tagsAsString = tags.map("'" + _ + "'").reduce((acc, it : String) => acc + "," + it)
      val query_set_tags = s"SET t.tags=[$tagsAsString] "
      val query_deactivated = "SET t.deactivatedAt={deactivatedAt}, " +
                                "t.active=false "
      val query_cu_location = "CREATE UNIQUE (t)-[:LOCATED_IN]->(ci), " +
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

    case PartupsChangedEvent(_, _id, name, tags, purpose, language, place_id, city, country, privacy_type, type_partup, phase, activity_count, end_date, deactivatedAt) =>
      val query_me_location = "MERGE (ci:City {_id: '{place_id}'}) " +
                                "ON CREATE SET ci.name= '{city}' " +
                                "MERGE (co:Country {name: '{country}'}) "
      val query_me_team ="MERGE (t:Team {_id:'{_id}'}) " +
                            "SET t.name='{name}', " +
                            "t.purpose='{purpose}', " +
                            "t.language='{language}', " +
                            "t.privacy_type={privacy_type}, " +
                            "t.type='{type_partup}', " +
                            "t.phase='{phase}, " +
                            "t.activity_count={activity_count}, " +
                            "t.end_date={end_date}' "
      val tagsAsString = tags.map("'" + _ + "'").reduce((acc, it : String) => acc + "," + it)
      val query_set_tags = s"SET t.tags=[$tagsAsString] "
      val query_deactivated = "SET t.deactivatedAt={deactivatedAt}, " +
                                "t.active=false "
      val query_cu_location = "CREATE UNIQUE (t)-[:LOCATED_IN]->(ci), " +
                                "(ci)-[:LOCATED_IN]->(co) "

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
        "MATCH (u:User {_id:'{_id}'), " +
           "(t:Team {_id:'{partup_id}'}) " +
           "CREATE UNIQUE (u)-[r:ACTIVE_IN]->(t) " +
           "SET r.contributions=1, " +
           "r.weight=1.5, " +
           "u.maxContributions=u.maxContributions+1, " +
           "t.partners=t.partners+1, " +
           "r.participation=1.5+(r.contributions/(toFloat(u.maxContributions)+0.00001)*2.0)+(r.comments/(toFloat(u.maxComments)+0.00001)*1.0)"
         ).on(("_id", _id), ("partup_id", partup_id))
        .execute()(conn)

    //Supporters
    case SupportersInsertedEvent(_, _id, partup_id) =>
      Cypher(
        "MERGE (u:User {_id:'{_id}'), " +
           "(t:Team {_id:'{partup_id}'}) " +
           "CREATE UNIQUE (u)-[r:ACTIVE_IN]->(t) " +
           "SET r.weight=1.0, " +
           "r.participation=1.0+(r.comments/(toFloat(u.maxComments)+0.00001)*1.0)"
         ).on(("_id", _id), ("partup_id", partup_id))
        .execute()(conn)

    case SupportersRemovedEvent(_, _id, partup_id) =>
      Cypher(
        "MATCH (u:User {_id:'{_id}'})-[r:ACTIVE_IN {weight:1.0}]->(t:Team {_id:'{partup_id}'}) " +
           "DELETE r"
         ).on(("_id", _id), ("partup_id", partup_id))
        .execute()(conn)

    //Members
    case MembersAcceptedEvent(_, _id, tribe_id) =>
      Cypher(
        "MERGE (u:User {_id:'{_id}'}), " +
          "(n:Network {_id:'{tribe_id}'}) " +
          "CREATE UNIQUE (u)-[:MEMBER_OF]->(t)"
        ).on(("_id", _id), ("tribe_id", tribe_id))
        .execute()(conn)

    case MembersInsertedEvent(_, _id, tribe_id) =>
      Cypher(
        "MERGE (u:User {_id:'{_id}'}), " +
          "(n:Network {_id:'{tribe_id}'}) " +
          "CREATE UNIQUE (u)-[:MEMBER_OF]->(n)"
        ).on(("_id", _id), ("tribe_id", tribe_id))
        .execute()(conn)

    case MembersRemovedEvent(_, _id, tribe_id) =>
      Cypher(
        "MATCH (u:User {_id:'{_id'})-[r:MEMBER_OF]->(n:Network {_id:'{tribe_id}'}) " +
          "DELETE r"
        ).on(("_id", _id), ("tribe_id", tribe_id))
        .execute()(conn)

    //Analytics
    case AnalyticsPageViewEvent(_, _id, partup_id) =>
      Cypher(
        "MATCH (u:User {_id:'{_id}'}-[r:ACTIVE_IN]->(t:Team {_id:'{'partup_id}'}) " +
          "SET r.pageViews=r.pageViews+1"
        ).on(("_id", _id), ("partup_id", partup_id))
        .execute()(conn)

    //Contributions
    case ContributionsInsertedEvent(_, _id, partup_id) =>
      Cypher(
        "MATCH (u:User {_id:'{_id}'})-[r:ACTIVE_IN]->(t:Team {_id:'{partup_id}'}) " +
          "WITH r.weight+(r.contributions/(toFloat(u.maxContributions)+0.00001)*2.0)+(r.comments/(toFloat(u.maxComments)+0.00001)*1.0) AS part, " +
          "r " +
          "SET r.contributions=r.contributions+1, " +
          "u.maxContributions=u.maxContributions+1, " +
          "r.participation=((REDUCE(avg=0, i IN r.ratings | avg + (i/20)))+part)/(LENGTH(r.ratings)+1)"
        ).on(("_id", _id), ("partup_id", partup_id))
        .execute()(conn)

    case ContributionsRemovedEvent(_, _id, partup_id) =>
      Cypher(
        "MATCH (u:User {_id:'{_id}'})-[r:ACTIVE_IN]->(t:Team {_id:'{partup_id}'})  " +
          "WITH r.weight+(r.contributions/(toFloat(u.maxContributions)+0.00001)*2.0)+(r.comments/(toFloat(u.maxComments)+0.00001)*1.0) AS part, " +
          "r " +
          "SET r.contributions=r.contributions-1, " +
          "u.maxContributions=u.maxContributions-1, " +
          "r.participation=((REDUCE(avg=0, i IN r.ratings | avg + (i/20)))+part)/(LENGTH(r.ratings)+1)"
        ).on(("_id", _id), ("partup_id", partup_id))
        .execute()(conn)

    //Comments
    case CommentsInsertedEvent(_, _id, partup_id) =>
      Cypher(
        "MATCH (u:User {_id:'{_id}'})-[r:ACTIVE_IN]->(t:Team {_id:'{partup_id}'}) " +
          "WITH r.weight+(r.contributions/(toFloat(u.maxContributions)+0.00001)*2.0)+(r.comments/(toFloat(u.maxComments)+0.00001)*1.0) AS part, " +
          "r " +
          "SET r.comments=r.comments+1, " +
          "u.maxComments=u.maxComments+1, " +
          "r.participation=((REDUCE(avg=0, i IN r.ratings | avg + (i/20)))+part)/(LENGTH(r.ratings)+1)"
        ).on(("_id", _id), ("partup_id", partup_id))
        .execute()(conn)

    case CommentsRemovedEvent(_, _id, partup_id) =>
      Cypher(
        "MATCH (u:User {_id:'{_id}'})-[r:ACTIVE_IN]->(t:Team {_id:'{partup_id}'}) " +
          "WITH r.weight+(r.contributions/(toFloat(u.maxContributions)+0.00001)*2.0)+(r.comments/(toFloat(u.maxComments)+0.00001)*1.0) AS part, " +
          "r " +
          "SET r.comments=r.comments-1, " +
          "u.maxComments=u.maxComments-1, " +
          "r.participation=((REDUCE(avg=0, i IN r.ratings | avg + (i/20)))+part)/(LENGTH(r.ratings)+1)"
        ).on(("_id", _id), ("partup_id", partup_id))
        .execute()(conn)

    //Ratings
    case RatingsInsertedEvent(_, _, user_id, partup_id, rating) =>
      Cypher(
        "MATCH (u:User {_id:'{_id}'})-[r:ACTIVE_IN]->(t:Team {_id:'{partup_id}'}) " +
          "WITH r.weight+(r.contributions/(toFloat(u.maxContributions)+0.00001)*2.0)+(r.comments/(toFloat(u.maxComments)+0.00001)*1.0) AS part, " +
          "r " +
          "SET r.ratings=r.ratings+[{rating}], " +
          "r.participation=((REDUCE(avg=0, i IN r.ratings | avg + (i/20)))+part)/(LENGTH(r.ratings)+1)"
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
