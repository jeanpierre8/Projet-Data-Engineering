object ReportUtils {
	case class Report (
		peacewatcher_id : String,
		peacewatcher_latitude : Int,
		peacewatcher_longitude : Int,
		surronding_citizen : List[(String, Int)],
		heard_words : List[String]
		)
}