ThisBuild / resolvers ++= Seq(
  "Apache Development Snapshot Repository" at "https://repository.apache.org/content/repositories/snapshots/",
  Resolver.mavenLocal
)

name := "ALSTestProject"

version := "0.1-SNAPSHOT"

organization := "dfki"

ThisBuild / scalaVersion := "2.11.12"

val flinkVersion = "1.4.2"
val elastic4sVersion = "5.5.11"

val flinkDependencies = Seq(
  "org.apache.flink" %% "flink-scala" % flinkVersion,
  "org.apache.flink" %% "flink-streaming-scala" % flinkVersion,
  "org.apache.flink" %% "flink-cep-scala" % flinkVersion,
  "org.apache.flink" %% "flink-ml" % flinkVersion)



val elasticsearchDependencies = Seq(
  "com.sksamuel.elastic4s" %% "elastic4s-core" % elastic4sVersion,
  // for the http client
  "com.sksamuel.elastic4s" %% "elastic4s-http" % elastic4sVersion,
  // for the tcp client
  "com.sksamuel.elastic4s" %% "elastic4s-tcp" % elastic4sVersion,
  // if you want to use reactive streams
  "com.sksamuel.elastic4s" %% "elastic4s-streams" % elastic4sVersion,
  // testing
  "com.sksamuel.elastic4s" %% "elastic4s-testkit" % elastic4sVersion % "test",
  "com.sksamuel.elastic4s" %% "elastic4s-embedded" % elastic4sVersion % "test"
)
excludeDependencies += "netty-all-4.1.10.Final"

lazy val root = (project in file(".")).
  settings(
    libraryDependencies ++= flinkDependencies,
    libraryDependencies ++= elasticsearchDependencies,
    libraryDependencies += "ch.qos.logback" % "logback-classic" % "1.2.3",
    libraryDependencies += "com.typesafe.scala-logging" %% "scala-logging" % "3.9.0"
  )
assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs@_*) =>
    xs map {
      _.toLowerCase
    } match {
      case "manifest.mf" :: Nil | "index.list" :: Nil |
           "dependencies" :: Nil => MergeStrategy.discard
      case _ => MergeStrategy.discard
    }
  case _ => MergeStrategy.first
}


assembly / mainClass := Some("dfki.MatrixDecompositionJob")

// make run command include the provided dependencies
Compile / run := Defaults.runTask(Compile / fullClasspath,
  Compile / run / mainClass,
  Compile / run / runner
).evaluated

// stays inside the sbt console when we press "ctrl-c" while a Flink programme executes with "run" or "runMain"
Compile / run / fork := true
Global / cancelable := true

// exclude Scala library from assembly
assembly / assemblyOption := (assembly / assemblyOption).value.copy(includeScala = false)
