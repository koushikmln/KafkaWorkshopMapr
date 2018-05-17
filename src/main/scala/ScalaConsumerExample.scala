import java.util.{Collections, Properties}
import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer}
import scala.collection.JavaConversions._

object ScalaConsumerExample {
  def main(args: Array[String]): Unit = {
    val props = new Properties()
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "rm01.itversity.com:6667,nn02.itversity.com:6667,nn01.itversity.com:6667")
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer")
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer")

    val consumer = new KafkaConsumer[String, String](props)
    consumer.subscribe(Collections.singletonList("Kafka-Testing"))
    while(true){
      val records = consumer.poll(500)
      for (record <- records.iterator()) {
        println("Received message: (" + record.key() + ", " + record.value() + ") at offset " + record.offset())
      }
    }
  }
}
