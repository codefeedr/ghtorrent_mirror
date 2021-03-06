ThisBuild / resolvers ++= Seq(
    "Apache Development Snapshot Repository" at "https://repository.apache.org/content/repositories/snapshots/",
    Resolver.mavenLocal,
    "Sonatype OSS Snapshots" at "https://oss.sonatype.org/content/repositories/snapshots"
)

name := "ghtorrent_mirror"
version := "1.0"
organization := "org.codefeedr"

ThisBuild / scalaVersion := "2.12.8"

val flinkVersion = "1.8.0"
val codefeedrVersion = "0.1.3"

val flinkDependencies = Seq(
  "org.apache.flink" %% "flink-scala" % flinkVersion % "provided",
  "org.apache.flink" %% "flink-streaming-scala" % flinkVersion % "provided")

val codefeedrDependencies = Seq(
  "org.codefeedr" %% "codefeedr-core" %  codefeedrVersion,
  "org.codefeedr" %% "codefeedr-plugin-ghtorrent" %  codefeedrVersion,
  "org.codefeedr" %% "codefeedr-plugin-elasticsearch" %  codefeedrVersion,
  "org.codefeedr" %% "codefeedr-plugin-mongodb" %  codefeedrVersion
)

val testDependencies = Seq(
  "org.scalactic" %% "scalactic" % "3.0.5",
  "org.scalatest" %% "scalatest" % "3.0.5" % "test"
)

lazy val root = (project in file(".")).
  settings(
    libraryDependencies ++= flinkDependencies,
    libraryDependencies ++= codefeedrDependencies,
    libraryDependencies ++= testDependencies
  )

assembly / mainClass := Some("org.codefeedr.Main")

// make run command include the provided dependencies
Compile / run  := Defaults.runTask(Compile / fullClasspath,
                                   Compile / run / mainClass,
                                   Compile / run / runner
                                  ).evaluated

// stays inside the sbt console when we press "ctrl-c" while a Flink programme executes with "run" or "runMain"
Compile / run / fork := true
Global / cancelable := true

// exclude Scala library from assembly
assembly / assemblyOption  := (assembly / assemblyOption).value.copy(includeScala = false)
