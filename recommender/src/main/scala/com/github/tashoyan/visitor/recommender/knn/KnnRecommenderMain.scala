package com.github.tashoyan.visitor.recommender.knn

import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.io.StdIn
import scala.util.Try

object KnnRecommenderMain extends KnnRecommenderArgParser {

  def main(args: Array[String]): Unit = {
    parser.parse(args, KnnRecommenderConfig()) match {
      case Some(config) => doMain(config)
      case None => sys.exit(1)
    }
  }

  private def doMain(implicit config: KnnRecommenderConfig): Unit = {
    implicit val spark: SparkSession = SparkSession.builder()
      .getOrCreate()

    Console.out.println(s"Actual configuration: $config")

    Console.out.println(
      """Enter ID of the person to be provided with recommendation and ID of the target region:
        | <person ID>[ <region ID>]
        |If region ID is omitted, then the home region of the person will be used.
      """.stripMargin
    )

    while (true) {
      val input = Try(
        StdIn.readLine("(CTRL-C for exit) <person ID>[ <region ID>]: ")
      )
      //TODO Complete
      println(input)
      val dummy = makeRecommendations(RecommenderTarget(0L, 0L))
      dummy.show()
    }
  }

  private def makeRecommendations(recommenderTarget: RecommenderTarget)(implicit spark: SparkSession, config: KnnRecommenderConfig): DataFrame = {
    val similarPersons = loadSimilarPersons(recommenderTarget.personId)
    val placeRatings = loadPlaceRatings
    //TODO Complete
    similarPersons.show()
    placeRatings.show()
    ???
  }

  private def loadSimilarPersons(personId: Long)(implicit spark: SparkSession, config: KnnRecommenderConfig): DataFrame = {
    val fileName = s"${config.samplesDir}/similar_persons"
    Console.out.println(s"Loading similar persons of person $personId from $fileName")
    spark.read
      .parquet(fileName)
      .where(col("person_id") === personId)
  }

  private def loadPlaceRatings(implicit spark: SparkSession, config: KnnRecommenderConfig): DataFrame = {
    val fileName = s"${config.samplesDir}/place_ratings"
    Console.out.println(s"Loading place ratings from $fileName")
    spark.read
      .parquet(fileName)
  }

  case class RecommenderTarget(
      personId: Long,
      targetRegionId: Long
  )
}
