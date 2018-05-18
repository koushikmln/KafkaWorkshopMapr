import com.typesafe.config.ConfigFactory
import kafka.serializer.StringDecoder
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.DStream
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.conf.Configuration

object SiteVisitCount {


  def getHbaseConnection(): Connection ={
    //Create Hbase Configuration Object
    val hBaseConf: Configuration = HBaseConfiguration.create()
    hBaseConf.set("hbase.zookeeper.quorum","mapr02.itversity.com,mapr03.itversity.com,mapr04.itversity.com")
    hBaseConf.set("hbase.zookeeper.property.clientPort","5181")
    hBaseConf.set("zookeeper.znode.parent","/hbase-unsecure")
    hBaseConf.set("hbase.cluster.distributed","true")
    //Establish Connection
    val connection = ConnectionFactory.createConnection(hBaseConf)
    connection
  }

  def insertOrUpdateMetrics(rowId: String, data: Int): Unit = {
    //Hbase Metadata
    val columnFamily1 = "metrics"
    val columnName11 = "last_timestamp"
    val columnName12 = "count"
    val connection = getHbaseConnection()

    val table = connection.getTable(TableName.valueOf("/user/mapr/log_data"))
    val row_get = new Get(Bytes.toBytes(rowId))
    //Insert Into Table
    val result = table.get(row_get)
    val value = result.getValue(Bytes.toBytes(columnFamily1),Bytes.toBytes(columnName12))
    if (value == null) {
      val row_put = new Put(Bytes.toBytes(rowId))
      row_put.addColumn(Bytes.toBytes(columnFamily1),Bytes.toBytes(columnName12),Bytes.toBytes(data.toString))
      table.put(row_put)
    } else {
      val count = Bytes.toString(value).toInt + data
      val row_put = new Put(Bytes.toBytes(rowId))
      row_put.addColumn(Bytes.toBytes(columnFamily1),Bytes.toBytes(columnName12),Bytes.toBytes(count.toString))
      table.put(row_put)
    }
    connection.close()
  }

  def main(args: Array[String]): Unit = {
    val conf = ConfigFactory.load
    val envProps = conf.getConfig(args(0))
    val sparkConf = new SparkConf().setMaster("yarn").setAppName("SiteTraffic")
    val streamingContext = new StreamingContext(sparkConf, Seconds(1))
    val topicsSet = Set("Kafka-Testing")

    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> envProps.getString("bootstrap.server"),
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "1",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )

    val logData: DStream[String] =KafkaUtils.createDirectStream[String, String](
      streamingContext,
      PreferConsistent,
      Subscribe[String, String](topicsSet, kafkaParams)
    ).map(record => record.value)

    val ipList = logData.map(line => {
      val ip = line.split(" ")(0)
      (ip, 1)
    }).reduceByKey(_ + _)

    // ipList.saveAsTextFiles("/user/mapr/kafka-testing/log")

    ipList.foreachRDD(ips =>{
      ips.foreach(ip =>{
        insertOrUpdateMetrics(ip._1, ip._2)
      })
    })

    streamingContext.start()
    streamingContext.awaitTermination()
  }

}
