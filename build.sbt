ThisBuild / scalaVersion := "2.12.12"

Test / parallelExecution := false

lazy val flink = (project in file("flink"))
  .settings(
    name := "flink",
    libraryDependencies ++= Seq(
      "com.typesafe.play" %% "play-json" % "2.9.2",
      "io.github.azhur" %% "kafka-serde-play-json" % "0.6.5",
      "org.apache.flink" % "flink-clients" % "1.15.1",
      "org.apache.flink" %% "flink-streaming-scala" % "1.15.1",
      "org.apache.flink" % "flink-connector-kafka" % "1.15.1",
      "org.specs2" %% "specs2-core" % "4.16.0" % Test,
      "org.apache.flink" % "flink-test-utils" % "1.15.1" % Test)
  )
