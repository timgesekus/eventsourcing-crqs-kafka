lazy val akkaHttpVersion = "10.1.4"
lazy val akkaVersion  = "2.5.16"

lazy val commonSettings = Seq(
  version := "0.1.0-SNAPSHOT",
  scalaVersion := "2.12.2",
  organization    := "de.dfs",
  resolvers += Resolver.bintrayRepo("cakesolutions", "maven"),
  libraryDependencies ++= Seq(
      "com.typesafe.akka" %% "akka-http"            % akkaHttpVersion,
      "com.typesafe.akka" %% "akka-http-spray-json" % akkaHttpVersion,
      "com.typesafe.akka" %% "akka-http-xml"        % akkaHttpVersion,
      "com.typesafe.akka" %% "akka-stream"          % akkaVersion,
      "net.cakesolutions" %% "scala-kafka-client" % "2.0.0",
      "net.cakesolutions" %% "scala-kafka-client-akka" % "2.0.0",
      "com.typesafe.akka" %% "akka-http-testkit"    % akkaHttpVersion % Test,
      "com.typesafe.akka" %% "akka-testkit"         % akkaVersion     % Test,
      "com.typesafe.akka" %% "akka-stream-testkit"  % akkaVersion     % Test,
      "org.scalatest"     %% "scalatest"            % "3.0.5"         % Test
  )
)


lazy val microservice = (project in file("kafka-aircraft-micro-service")).
  dependsOn(protocol).
  settings(
    commonSettings,
    name := "kafka-aircraft-micro-service"
  )

lazy val counter = (project in file("kafka-aircraft-counter")).
  dependsOn(protocol).
  settings(
    commonSettings,
    name := "kafka-aircraft-micro-service"
  )

lazy val protocol = (project in file("kafka-aircraft-protocol")).
  settings(
    commonSettings,
    name := "kafka-aircraft-protocol"
  )

