organization := "com.partup"

version := "0.1"

scalaVersion := "2.11.6"

ivyScala := ivyScala.value map { _.copy(overrideScalaVersion = true) }

scalacOptions := Seq("-unchecked", "-deprecation", "-encoding", "utf8")

assemblyOutputPath in assembly := file("target/api-with-dependencies.jar")

libraryDependencies ++= {
  val akkaV = "2.3.9"
  val sprayV = "1.3.3"
  Seq(
    "org.mongodb.scala" %% "mongo-scala-driver" % "1.0.0",
    "io.spray" %% "spray-can" % sprayV,
    "io.spray" %% "spray-routing" % sprayV,
    "io.spray" %% "spray-json" % "1.3.2",
    "io.spray" %% "spray-testkit" % sprayV % "test",
    "com.typesafe.akka" %% "akka-actor" % akkaV,
    "com.typesafe.akka" %% "akka-testkit" % akkaV % "test",
    "org.specs2" %% "specs2-core" % "2.3.11" % "test"
  )
}

Revolver.settings
