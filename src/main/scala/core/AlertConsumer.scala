import ReportUtils.Report
import scala.annotation.tailrec
import scala.io.Source
import java.util
import org.apache.kafka.clients.consumer.KafkaConsumer
import java.util.Properties
import scala.collection.JavaConverters._

object AlertConsumer {
	def main(args: Array[String]): Unit = {
		val props = new Properties()
		props.put("bootstrap.servers", "localhost:9092")
		props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
		props.put("value.deserializer", "ReportDeserializer")
		props.put("auto.offset.reset", "latest")
		props.put("group.id", "consumer-group")
		val consumer: KafkaConsumer[String, Report] = new KafkaConsumer[String, Report](props)
		consumer.subscribe(util.Arrays.asList("quickstart-events"))
		processStream(consumer)
	}
	@tailrec
	def processStream(consumer: KafkaConsumer[String, Report]) : Unit= {
		val record = consumer.poll(1000).asScala
		record.iterator foreach println
		processStream(consumer)	
	}
}
