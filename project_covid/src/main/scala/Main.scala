import scala.io.Source
import java.io.PrintWriter
import scala.collection.parallel.CollectionConverters._

object CovidAnalysis {

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
}


@main def covidinru(): Unit = {

  val functionalTime = CovidAnalysis.totalImpact("covid19-russia-cases-scrf.csv", "result_functional.json")

  val parallelTime = CovidAnalysis.totalImpactParallel("covid19-russia-cases-scrf.csv", "result_parallel.json")

  println("Execution Time Comparison")
  println("-------------------------")
  println("Total Impact (Daily confirmed + Death + Recovered)")
  println(f"Functional Processing : $functionalTime%.2f ms")
  println(f"Parallel Processing   : $parallelTime%.2f ms")

}