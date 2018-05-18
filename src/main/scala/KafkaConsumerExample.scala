import java.util.{Collections, Properties}

import com.typesafe.config.ConfigFactory
import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer}

import scala.collection.JavaConversions._

object KafkaConsumerExample {
  def main(args: Array[String]): Unit = {
    val conf = ConfigFactory.load
    val envProps = conf.getConfig(args(0))
    val props = new Properties()
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, envProps.getString("bootstrap.server"))
    props.put(ConsumerConfig.GROUP_ID_CONFIG, "1")
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
