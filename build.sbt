name := "KafkaWorkshopMapr"

version := "0.1"

scalaVersion := "2.11.11"


libraryDependencies += "org.apache.spark" % "spark-core_2.11" % "2.2.1"
libraryDependencies += "org.apache.spark" % "spark-sql_2.11" % "2.2.1"
libraryDependencies += "org.apache.spark" % "spark-streaming_2.11" % "2.2.1"
libraryDependencies += "org.apache.spark" %% "spark-streaming-kafka-0-10" % "2.2.1"
libraryDependencies += "org.apache.hbase" % "hbase-client" % "1.1.8"
libraryDependencies += "org.apache.hbase" % "hbase-common" % "1.1.8"
libraryDependencies += "org.apache.kafka" %% "kafka" % "1.1.0"
libraryDependencies += "org.apache.kafka" % "kafka-clients" % "1.1.0"
libraryDependencies += "org.apache.hadoop" % "hadoop-common" % "2.7.0"
libraryDependencies += "com.typesafe" % "config" % "1.3.2"