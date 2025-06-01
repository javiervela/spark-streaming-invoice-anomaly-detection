name := "anomalyDetection"

version := "1.0"

scalaVersion := "2.11.8"

libraryDependencies ++= Seq(
"org.apache.spark" %% "spark-core" % "2.3.1" % "provided",
"org.apache.spark" %% "spark-sql" % "2.3.1" % "provided",
"org.apache.spark" %% "spark-streaming-kafka-0-8" % "2.3.1",
"org.apache.spark" %% "spark-mllib" % "2.3.1" % "provided",
"com.databricks" %% "spark-csv" % "1.5.0",
"org.slf4j" % "slf4j-simple" % "1.7.2",
"com.univocity" % "univocity-parsers" % "1.5.1"
  // 	org.apache.kafka » kafka_2.10	0.8.2.1	0.10.2.0
  //[info] Including from cache: kafka_2.10-0.8.2.1.jar
  //[info] Including from cache: metrics-core-2.2.0.jar
  //[info] Including from cache: kafka-clients-0.8.2.1.jar
)

resolvers += "Cloudera" at "https://repository.cloudera.com/artifactory/cloudera-repos/"

assemblyMergeStrategy in assembly := {
  case m if m.toLowerCase.endsWith("manifest.mf")          => MergeStrategy.discard
  case m if m.toLowerCase.matches("meta-inf.*\\.sf$")      => MergeStrategy.discard
  case "log4j.properties"                                  => MergeStrategy.discard
  case m if m.toLowerCase.startsWith("meta-inf/services/") => MergeStrategy.filterDistinctLines
  case "reference.conf"                                    => MergeStrategy.concat
  case _                                                   => MergeStrategy.first
}

