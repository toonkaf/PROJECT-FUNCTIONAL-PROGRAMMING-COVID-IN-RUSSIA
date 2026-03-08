import scala.io.Source
import java.io.PrintWriter
import scala.collection.parallel.CollectionConverters._
case class CovidRecord(date: String, confirmed: Double, deaths: Double, recovered: Double)

object CovidAnalysis {
  def parseLine(line: String): Option[CovidRecord] = {
    val c = line.split(",").map(_.trim)
    if (c.length < 7) return None

    import scala.util.{Try, Success, Failure}
    Try {
      CovidRecord(c(0), c(4).toDouble, c(5).toDouble, c(6).toDouble)
    } match {
      case Success(r) => Some(r)
      case Failure(_) => None
    }
  }

  def totalImpact(inputFile: String, outputFile: String): Double = {

    val start = System.nanoTime()

    val json =
      Source.fromFile(inputFile)
        .getLines()
        .drop(1)
        .map { line =>
          val c = line.split(",")

          val date = c(0)
          val region = c(2)

          val confirmed = c(4).toDouble
          val deaths = c(5).toDouble
          val recovered = c(6).toDouble

          val total = confirmed + deaths + recovered

          s"""{"date":"$date","region":"$region","totalImpact":$total}"""
        }
        .mkString("[\n  ", ",\n  ", "\n]")

    new PrintWriter(outputFile) {
      write(json)
      close()
    }

    val end = System.nanoTime()
    (end - start) / 1e6
  }


  // Parallel version
  def totalImpactParallel(inputFile: String, outputFile: String): Double = {

    val start = System.nanoTime()

    val lines = Source.fromFile(inputFile).getLines().drop(1).toList

    val json =
      lines.par
        .map { line =>
          val c = line.split(",")

          val date = c(0)
          val region = c(2)

          val confirmed = c(4).toDouble
          val deaths = c(5).toDouble
          val recovered = c(6).toDouble

          val total = confirmed + deaths + recovered

          s"""{"date":"$date","region":"$region","totalImpact":$total}"""
        }
        .mkString("[\n  ", ",\n  ", "\n]")

    new PrintWriter(outputFile) {
      write(json)
      close()
    }

    val end = System.nanoTime()
    (end - start) / 1e6
  }

  // Yasmina
  def dailySummary(inputFile: String, outputFile: String): Double = {
    val start = System.nanoTime()
    
    val lines = scala.io.Source.fromFile(inputFile).getLines().drop(1).toList
    
    val summaryResults = lines
      .flatMap(parseLine)
      .groupBy(_.date)
      .map { case (date, records) =>
        val totalC = records.map(_.confirmed).sum
        val totalD = records.map(_.deaths).sum
        val totalR = records.map(_.recovered).sum

        s"""{"date":"$date","totalConfirmed":$totalC,"totalDeaths":$totalD,"totalRecovered":$totalR}"""
      }
      .toList.sorted

    new PrintWriter(outputFile) {
      write(summaryResults.mkString("[\n  ", ",\n  ", "\n]"))
      close()
    }

    val end = System.nanoTime()
    (end - start) / 1e6
  }

  // Parallel version
  def dailySummaryParallel(inputFile: String, outputFile: String): Double = {
    val start = System.nanoTime()
    
    val lines = scala.io.Source.fromFile(inputFile).getLines().drop(1).toList
    
    val summaryResults = lines.par
      .flatMap(parseLine)
      .groupBy(_.date)
      .map { case (date, records) =>
        val totalC = records.map(_.confirmed).sum
        val totalD = records.map(_.deaths).sum
        val totalR = records.map(_.recovered).sum
        s"""{"date":"$date","totalConfirmed":$totalC,"totalDeaths":$totalD,"totalRecovered":$totalR}"""
      }
      .toList.sorted

    new PrintWriter(outputFile) {
      write(summaryResults.mkString("[\n  ", ",\n  ", "\n]"))
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
  println(f"Functional Processing : $functionalTime%.2f ms")
  println(f"Parallel Processing   : $parallelTime%.2f ms")

  val dailyfunctionalTime = CovidAnalysis.dailySummary(csvFile, "daily_summary_functional.json")
  val dailyparallelTime = CovidAnalysis.dailySummaryParallel(csvFile, "daily_summary_parallel.json")

  println("\nDaily Summary Aggregation (confirmed + Death + Recovered)")
  println(f"Functional Processing : $dailyfunctionalTime%.2f ms")
  println(f"Parallel Processing   : $dailyparallelTime%.2f ms")
}