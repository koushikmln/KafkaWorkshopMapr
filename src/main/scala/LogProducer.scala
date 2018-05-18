import java.util.Properties

import com.typesafe.config.ConfigFactory
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}

import scala.sys.process._

object LogProducer {
  def main(args: Array[String]): Unit = {
    val conf = ConfigFactory.load
    val envConf = conf.getConfig(args(0))
    val props = new Properties()
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, envConf.getString("bootstrap.server"))
    props.put(ProducerConfig.CLIENT_ID_CONFIG, "ScalaProducerExample")
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
    val producer = new KafkaProducer[String, String](props)

    def processLine(line: String, producer: KafkaProducer[String, String]): Unit = {
      val data = new ProducerRecord[String, String]("Kafka-Testing", "Key", line)
      producer.send(data)
    }

    val file = "/opt/gen_logs/logs/access.log"

    val tail = Seq("tail", "-f", file)

    tail.lineStream.foreach(processLine(_, producer))
  }
}
