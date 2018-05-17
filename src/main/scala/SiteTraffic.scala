import kafka.serializer.StringDecoder
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.conf.Configuration

object SiteTraffic {


  def getHbaseConnection(): Connection ={
    //Create Hbase Configuration Object
    val hBaseConf: Configuration = HBaseConfiguration.create()
    hBaseConf.set("hbase.zookeeper.quorum","nn01.itversity.com,nn02.itversity.com,rm01.itversity.com")
    hBaseConf.set("hbase.zookeeper.property.clientPort","2181")
    hBaseConf.set("zookeeper.znode.parent","/hbase-unsecure")
    hBaseConf.set("hbase.cluster.distributed","true")
    //Establish Connection
    val connection = ConnectionFactory.createConnection(hBaseConf)
    connection
  }

  def insertOrUpdateMetrics(rowId: String, data: String): Unit = {
    //Hbase Metadata
    val columnFamily1 = "metrics"
    val columnName11 = "last_timestamp"

    val connection = getHbaseConnection()

    val table = connection.getTable(TableName.valueOf("log_data"))
    val row_get = new Get(Bytes.toBytes(rowId))
    //Insert Into Table
    val result = table.get(row_get)
    if (result.isEmpty) {
      val row_put = new Put(Bytes.toBytes(rowId))
      row_put.addColumn(Bytes.toBytes(columnFamily1),Bytes.toBytes(columnName11),Bytes.toBytes(data))
      table.put(row_put)
    } else {
      // Same Logic right now
      val row_put = new Put(Bytes.toBytes(rowId))
      row_put.addColumn(Bytes.toBytes(columnFamily1),Bytes.toBytes(columnName11),Bytes.toBytes(data))
      table.put(row_put)
    }
    connection.close()
  }

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setAppName("kafka-testing").setMaster("yarn-client")
    val streamingContext = new StreamingContext(sparkConf, Seconds(20))
    val topicsSet = Set("Kafka-Testing")
    val kafkaParams = Map[String, String]("metadata.broker.list" -> "rm01.itversity.com:6667,nn02.itversity.com:6667,nn01.itversity.com:6667")
    val logData: DStream[String] = KafkaUtils.
      createDirectStream[String, String, StringDecoder, StringDecoder](
      streamingContext, kafkaParams, topicsSet).map(_._2)

    val ipList = logData.map(line => {
      val ip = line.split(" ")(0)
      val timestamp = line.split(" ")(3).replace("[","").replace("]","")
      ip + "," + timestamp
    })

    ipList.saveAsTextFiles("hdfs://nn01.itversity.com:8020/user/koushikmln/kafka-testing/log")

    ipList.foreachRDD(ips =>{
      ips.foreach(ip =>{
        insertOrUpdateMetrics(ip.split(",")(0), ip.split(",")(1))
      })
    })

    streamingContext.start()
    streamingContext.awaitTermination()
  }

}
