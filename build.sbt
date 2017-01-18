val sparkVersion = "1.6.2"
val mongoSparkConnectorVersion  = "1.1.0"

lazy val root = (project in file("."))
  .settings(
    name := "BusinessRecommendations",
    version := "1.0",
    scalaVersion := "2.10.6",
    libraryDependencies ++= Seq(
      "org.apache.spark" %% "spark-core" % sparkVersion % "provided",
      "org.apache.spark" %% "spark-mllib" % sparkVersion % "provided",
      "org.apache.spark" %% "spark-sql" % sparkVersion % "provided",
      "org.mongodb.spark" %% "mongo-spark-connector" % mongoSparkConnectorVersion  % "provided"
    ),
    resolvers += "Sonatype snapshots" at "https://oss.sonatype.org/content/repositories/snapshots"
  )
