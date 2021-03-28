import ReportUtils.Report
import scala.io.Source
import scala.util.Random
import scala.annotation.tailrec
import java.util.Properties
import org.apache.kafka.clients.producer._

object DroneProducer {
	def main(args: Array[String]): Unit = {
		val words = Source.fromFile("resources/dico_fr.txt").getLines.toArray
		val names = Source.fromFile("resources/names.txt").getLines.toArray
		val r = Random
		val report = random_report(r, words, names)
		val props = new Properties()
		props.put("bootstrap.servers", "localhost:9092")
		props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
		props.put("value.serializer", "ReportSerializer")
		val producer = new KafkaProducer[String, Report](props)
		val record = new ProducerRecord[String, Report]("quickstart-events", "key", report)
		producer.send(record)
		producer.close()
	}

	def random_list_reports(r : Random, names : Array[String], words : Array[String], length : Int) : List[Report] = {
		random_list_repo(List(), r, names, words, length)
	}

	@tailrec
	def random_list_repo(tail : List[Report], r : Random, names : Array[String], words : Array[String], length : Int) : List[Report] = {
		if (length <= 0) tail
		else random_list_repo(random_report(r, names, words) +: tail, r, names, words, length - 1)
	}

	def random_report(r : Random, names : Array[String], words : Array[String]) : Report = {
		Report(random_id(r, 6), r.nextInt(420), r.nextInt(420), random_list_citizen(r, names, 10), random_list_words(r, words, 10))
	}


	def random_id(r : Random, length : Int) : String = {
		random_idr("", r, length)
	}

	@tailrec
	def random_idr(tail : String, r : Random, length : Int) : String = {
		if (length <= 0) tail
		else random_idr((r.nextInt(10) + 48).toChar +: tail, r, length - 1)
	}


	def random_list_citizen(r : Random, names : Array[String], max_length : Int) : List[(String, Int)] = {
		val length = r.nextInt(max_length)
		random_list_ci(List(), r, names, length)
	}

	@tailrec
	def random_list_ci(tail : List[(String, Int)], r : Random, names : Array[String], length : Int) : List[(String, Int)] = {
		if (length <= 0) tail
		else random_list_ci((names(r.nextInt(names.size)), r.nextInt(101)) :: tail, r, names, length - 1)
	}


	def random_list_words(r : Random, words : Array[String], max_length : Int) : List[String] = {
		val length = r.nextInt(max_length)
		random_list_str(List(), r, words, length)
	}

	@tailrec
	def random_list_str(tail : List[String], r : Random, words : Array[String], length : Int) : List[String] = {
		if (length <= 0) tail
		else random_list_str(words(r.nextInt(words.size)) :: tail, r, words, length - 1)
	}
}
