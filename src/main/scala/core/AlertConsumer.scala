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
		val record = consumer.poll(0).asScala
		record.iterator foreach {
			record => {
				if (contains_bad_peacescore(record.value.surronding_citizen)) {
					println(record)
				}
			}
		}
		processStream(consumer)
	}

	@tailrec
	def contains_bad_peacescore(citizens : List[(String, Int)]) : Boolean = citizens match {
		case Nil => false
		case head :: Nil => head._2 < 10
		case head :: tail => head._2 < 10 || contains_bad_peacescore(tail)
	}
}
