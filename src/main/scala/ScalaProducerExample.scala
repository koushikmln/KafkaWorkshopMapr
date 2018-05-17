import java.util.{Properties}
import org.apache.kafka.clients.producer.{ProducerConfig, KafkaProducer, ProducerRecord}

object ScalaProducerExample {
  def main(args: Array[String]): Unit = {
    val props = new Properties()
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "rm01.itversity.com:6667,nn02.itversity.com:6667,nn01.itversity.com:6667")
    props.put(ProducerConfig.CLIENT_ID_CONFIG, "ScalaProducerExample")
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
    val producer = new KafkaProducer[String, String](props)

    val data = new ProducerRecord[String, String]("Kafka-Testing", "Key", "Value")
    producer.send(data)
    producer.close()
  }
}
