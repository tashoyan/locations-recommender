package com.github.tashoyan.recommender.knn

import com.github.tashoyan.recommender.{DataUtils, RecommenderMainCommon}
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

    val persons = DataUtils.loadPersons(config.dataDir)
    val places = DataUtils.loadPlaces(config.dataDir)

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
    val placeRatingVectors = loadPlaceRatingVectors(regionIds)
    val categoryRatingVectors = loadCategoryRatingVectors(regionIds)
    val placeRatings = loadPlaceRatings(regionIds)
    val recommender = new KnnRecommender(
      placeRatingVectors = placeRatingVectors,
      categoryRatingVectors = categoryRatingVectors,
      placeRatings = placeRatings,
      placeWeight = config.placeWeight,
      categoryWeight = config.categoryWeight,
      kNearest = config.kNearest
    )
    Try(recommender.makeRecommendations(recommenderTarget.personId))
  }

  private def loadPlaceRatingVectors(regionIds: Seq[Long])(implicit spark: SparkSession, config: KnnRecommenderConfig): DataFrame = {
    val fileName = DataUtils.generatePlaceRatingVectorsFileName(regionIds, config.dataDir)
    Console.out.println(s"Loading place rating vectors from $fileName")
    spark.read
      .parquet(fileName)
  }

  private def loadCategoryRatingVectors(regionIds: Seq[Long])(implicit spark: SparkSession, config: KnnRecommenderConfig): DataFrame = {
    val fileName = DataUtils.generateCategoryRatingVectorsFileName(regionIds, config.dataDir)
    Console.out.println(s"Loading category rating vectors from $fileName")
    spark.read
      .parquet(fileName)
  }

  private def loadPlaceRatings(regionIds: Seq[Long])(implicit spark: SparkSession, config: KnnRecommenderConfig): DataFrame = {
    val fileName = DataUtils.generatePlaceRatingsFileName(regionIds, config.dataDir)
    Console.out.println(s"Loading place ratings from $fileName")
    spark.read
      .parquet(fileName)
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
    val t0 = System.currentTimeMillis()
    recommendedPlaces.show(false)
    println(s"Done in ${System.currentTimeMillis() - t0} milliseconds")
  }

}
