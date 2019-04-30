package com.github.tashoyan.visitor.recommender.knn

import com.github.tashoyan.visitor.recommender.{DataUtils, RecommenderMainCommon}
import org.apache.spark.sql.functions.{broadcast, col}
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.io.StdIn
import scala.util.{Failure, Success, Try}

object KnnRecommenderMain extends KnnRecommenderArgParser with RecommenderMainCommon {

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

    val persons = loadPersons(config.samplesDir)
    val places = loadPlaces(config.samplesDir)

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

      val parsedInput = input map parseInput
      val tryRecommenderTarget = parsedInput flatMap calcRecommenderTarget(persons)
      val tryRecommendations = tryRecommenderTarget flatMap makeRecommendations

      tryRecommendations match {
        case Success(recommendations) =>
          printRecommendations(tryRecommenderTarget.get, recommendations, places)
        case Failure(e) =>
          Console.err.println(e.getClass.getSimpleName + ": " + e.getMessage)
      }
    }
  }

  private def makeRecommendations(recommenderTarget: RecommenderTarget)(implicit spark: SparkSession, config: KnnRecommenderConfig): Try[DataFrame] = {
    val regionIds = Seq(recommenderTarget.homeRegionId, recommenderTarget.targetRegionId)
    val similarPersons = loadSimilarPersons(regionIds)
    val placeRatings = loadPlaceRatings(regionIds)
    val recommender = new KnnRecommender(similarPersons, placeRatings)
    Try(recommender.makeRecommendations(recommenderTarget.personId))
  }

  private def printRecommendations(
      recommenderTarget: RecommenderTarget,
      recommendations: DataFrame,
      places: DataFrame
  )(implicit config: KnnRecommenderConfig): Unit = {
    val targetRegionId = recommenderTarget.targetRegionId
    val targetRegionPlaces = places
      .where(col("region_id") === targetRegionId)
    val recommendedPlaces = targetRegionPlaces
      .join(broadcast(recommendations), col("id") === col("place_id"))
      .orderBy(col("estimated_rating").desc)
      .limit(config.maxRecommendations)

    Console.out.println(s"Person ${recommenderTarget.personId} might want to visit in region $targetRegionId:")
    recommendedPlaces.show(false)
  }

  private def loadSimilarPersons(regionIds: Seq[Long])(implicit spark: SparkSession, config: KnnRecommenderConfig): DataFrame = {
    val fileName = DataUtils.generateSimilarPersonsFileName(regionIds, config.samplesDir)
    Console.out.println(s"Loading similar persons from $fileName")
    spark.read
      .parquet(fileName)
  }

  private def loadPlaceRatings(regionIds: Seq[Long])(implicit spark: SparkSession, config: KnnRecommenderConfig): DataFrame = {
    val fileName = DataUtils.generatePlaceRatingsFileName(regionIds, config.samplesDir)
    Console.out.println(s"Loading place ratings from $fileName")
    spark.read
      .parquet(fileName)
  }

}
