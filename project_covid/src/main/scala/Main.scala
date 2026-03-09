import scala.io.Source
import java.io.PrintWriter
import scala.util.Try
import scala.collection.parallel.CollectionConverters._

case class CovidRecord(date: String, region: String, confirmed: Double, deaths: Double, recovered: Double)

object CovidAnalysis {

  def parseLine(line: String): Either[String, CovidRecord] = {

    val c = line.split(",").map(_.trim)

    if (c.length < 7) Left("Invalid column length")
    else {

      val parsed =
        for {
          confirmed <- Try(c(4).toDouble).toOption
          deaths <- Try(c(5).toDouble).toOption
          recovered <- Try(c(6).toDouble).toOption
        } yield CovidRecord(c(0), c(2), confirmed, deaths, recovered)

      parsed.toRight(s"Invalid numeric value: $line")
    }
  }

  def toJsonArray(data: Seq[String]): String =
    data.mkString("[\n  ", ",\n  ", "\n]")

  //Ratchanon
  def totalImpact(inputFile: String, outputFile: String): Double = {

    val lines = Source.fromFile(inputFile).getLines().drop(1).toList

    val start = System.nanoTime()

    val records =
      lines
        .map(parseLine)
        .collect { case Right(r) => r }

    val json =
      records
        .map { r =>
          val total = r.confirmed + r.deaths + r.recovered
          s"""{"date":"${r.date}","region":"${r.region}","totalImpact":$total}"""
        }

    new PrintWriter(outputFile) {
      write(toJsonArray(json))
      close()
    }

    val end = System.nanoTime()
    (end - start) / 1e6
  }


  def totalImpactParallel(inputFile: String, outputFile: String): Double = {

    val lines = Source.fromFile(inputFile).getLines().drop(1).toList

    val start = System.nanoTime()

    val records =
      lines
        .map(parseLine)
        .collect { case Right(r) => r }

    val json =
      records.par
        .map { r =>
          val total = r.confirmed + r.deaths + r.recovered
          s"""{"date":"${r.date}","region":"${r.region}","totalImpact":$total}"""
        }
        .toList

    new PrintWriter(outputFile) {
      write(toJsonArray(json))
      close()
    }

    val end = System.nanoTime()
    (end - start) / 1e6
  }

  // Yasmina
  def dailySummary(inputFile: String, outputFile: String): Double = {

    val start = System.nanoTime()

    val lines = Source.fromFile(inputFile).getLines().drop(1).toList

    val records =
      lines
        .map(parseLine)
        .collect { case Right(r) => r }

    val summaryResults =
      records
        .groupBy(_.date)
        .map { case (date, rs) =>

          val totalC = rs.map(_.confirmed).sum
          val totalD = rs.map(_.deaths).sum
          val totalR = rs.map(_.recovered).sum

          s"""{"date":"$date","totalConfirmed":$totalC,"totalDeaths":$totalD,"totalRecovered":$totalR}"""
        }
        .toList
        .sorted

    new PrintWriter(outputFile) {
      write(toJsonArray(summaryResults))
      close()
    }

    val end = System.nanoTime()
    (end - start) / 1e6
  }

  // Parallel version
  def dailySummaryParallel(inputFile: String, outputFile: String): Double = {

    val start = System.nanoTime()

    val lines = Source.fromFile(inputFile).getLines().drop(1).toList

    val records =
      lines
        .map(parseLine)
        .collect { case Right(r) => r }

    val summaryResults =
      records.par
        .groupBy(_.date)
        .map { case (date, rs) =>

          val totalC = rs.map(_.confirmed).sum
          val totalD = rs.map(_.deaths).sum
          val totalR = rs.map(_.recovered).sum

          s"""{"date":"$date","totalConfirmed":$totalC,"totalDeaths":$totalD,"totalRecovered":$totalR}"""
        }
        .toList
        .sorted

    new PrintWriter(outputFile) {
      write(toJsonArray(summaryResults))
      close()
    }

    val end = System.nanoTime()
    (end - start) / 1e6
  }

  // Puriwaj
  def dailyCitySummary(inputFile: String, outputFile: String): Double = {

    val start = System.nanoTime()

    val lines = Source.fromFile(inputFile).getLines().drop(1).toList

    val records =
      lines
        .map(parseLine)
        .collect { case Right(r) => r }

    val result =
      records
        .groupBy(r => (r.date, r.region))
        .map { case ((date, region), rs) =>

          val confirmed = rs.map(_.confirmed).sum
          val deaths = rs.map(_.deaths).sum
          val recovered = rs.map(_.recovered).sum

          s"""{"date":"$date","region":"$region","confirmed":$confirmed,"deaths":$deaths,"recovered":$recovered}"""
        }
        .toList
        .sorted

    new PrintWriter(outputFile) {
      write(toJsonArray(result))
      close()
    }

    val end = System.nanoTime()
    (end - start) / 1e6
  }

  def dailyCitySummaryParallel(inputFile: String, outputFile: String): Double = {

    val start = System.nanoTime()

    val lines = Source.fromFile(inputFile).getLines().drop(1).toList

    val records =
      lines
        .map(parseLine)
        .collect { case Right(r) => r }

    val result =
      records.par
        .groupBy(r => (r.date, r.region))
        .map { case ((date, region), rs) =>

          val confirmed = rs.map(_.confirmed).sum
          val deaths = rs.map(_.deaths).sum
          val recovered = rs.map(_.recovered).sum

          s"""{"date":"$date","region":"$region","confirmed":$confirmed,"deaths":$deaths,"recovered":$recovered}"""
        }
        .toList
        .sorted

    new PrintWriter(outputFile) {
      write(toJsonArray(result))
      close()
    }

    val end = System.nanoTime()
    (end - start) / 1e6
  }
}


@main def covidinru(): Unit = {

  val csvFile = "covid19-russia-cases-scrf.csv"

  val functionalTime = CovidAnalysis.totalImpact(csvFile, "result_functional.json")

  val parallelTime = CovidAnalysis.totalImpactParallel(csvFile, "result_parallel.json")

  println("Execution Time Comparison")
  println("-------------------------")
  println("Total Impact (Daily confirmed + Death + Recovered)")
  println(f"Sequential Processing : $functionalTime%.2f ms")
  println(f"Parallel Processing   : $parallelTime%.2f ms")

  val dailyfunctionalTime = CovidAnalysis.dailySummary(csvFile, "daily_summary_functional.json")

  val dailyparallelTime = CovidAnalysis.dailySummaryParallel(csvFile, "daily_summary_parallel.json")

  println("\nDaily Summary Aggregation (confirmed + Death + Recovered)")
  println(f"Sequential Processing : $dailyfunctionalTime%.2f ms")
  println(f"Parallel Processing   : $dailyparallelTime%.2f ms")

  println("\nDaily City Summary (confirmed, deaths, recovered per city per day)")

  val cityTime = CovidAnalysis.dailyCitySummary(csvFile, "daily_city_functional.json")

  val cityParallelTime = CovidAnalysis.dailyCitySummaryParallel(csvFile, "daily_city_parallel.json")

  println(f"Sequential Processing : $cityTime%.2f ms")
  println(f"Parallel Processing   : $cityParallelTime%.2f ms")
}