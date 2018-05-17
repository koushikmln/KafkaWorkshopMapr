name := "KafkaWorkshop"

version := "0.1"

scalaVersion := "2.10.7"


libraryDependencies += "org.apache.spark" % "spark-core_2.10" % "1.6.2"
libraryDependencies += "org.apache.spark" % "spark-sql_2.10" % "1.6.2"
libraryDependencies += "org.apache.spark" % "spark-streaming_2.10" % "1.6.2"
libraryDependencies += "org.apache.spark" % "spark-streaming-kafka_2.10" % "1.6.2"
libraryDependencies += "org.apache.hbase" % "hbase-client" % "1.1.2"
libraryDependencies += "org.apache.hbase" % "hbase-common" % "1.1.2"
libraryDependencies += "org.apache.kafka" % "kafka_2.10" % "0.8.0"
libraryDependencies += "org.apache.kafka" % "kafka-clients" % "0.10.2.1"
libraryDependencies += "org.apache.hadoop" % "hadoop-common" % "2.7.3"
libraryDependencies += "org.apache.hadoop" % "hadoop-client" % "2.7.3"