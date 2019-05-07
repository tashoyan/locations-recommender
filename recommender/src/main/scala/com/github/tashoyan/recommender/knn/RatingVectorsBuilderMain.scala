package com.github.tashoyan.recommender.knn

import com.github.tashoyan.recommender.{DataUtils, PlaceVisits}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

object RatingVectorsBuilderMain extends RatingVectorsBuilderArgParser with PlaceVisits {

  def main(args: Array[String]): Unit = {
    parser.parse(args, RatingVectorsBuilderConfig()) match {
      case Some(config) => doMain(config)
      case None => sys.exit(1)
    }
  }

  private def doMain(implicit config: RatingVectorsBuilderConfig): Unit = {
    implicit val spark: SparkSession = SparkSession.builder()
      .getOrCreate()

    Console.out.println(s"Actual configuration: $config")

    val locationVisits = DataUtils.loadLocationVisits(config.dataDir)
    val places = DataUtils.loadPlaces(config.dataDir)

    Console.out.println("Generating place visits")
    val placeVisits = calcPlaceVisits(locationVisits, places, config.lastDaysCount)
      .cache()
    //    printPlaceVisits(placeVisits)
    writePlaceVisits(placeVisits, config.dataDir)

    generateRegionRatingVectors(placeVisits, places)
  }

  private def generateRegionRatingVectors(
      placeVisits: DataFrame,
      places: DataFrame
  )(implicit spark: SparkSession, config: RatingVectorsBuilderConfig): Unit = {
    val regionIds = extractRegionIds(places)
    val regionsPlaceVisits = extractRegionsPlaceVisits(placeVisits, regionIds)

    regionsPlaceVisits.foreach { case (regIds, regPlaceVisits) =>
      val regPlaceRatings = RatingsBuilder.calcRatings(
        placeVisits = regPlaceVisits,
        entityIdColumn = "place_id",
        ratingColumn = "rating",
        topN = config.maxRatedPlaces
      )
      val regCategoryRatings = RatingsBuilder.calcRatings(
        placeVisits = regPlaceVisits,
        entityIdColumn = "category_id",
        ratingColumn = "rating",
        topN = config.maxRatedCategories
      )

      val regPlaceRatingVectors = RatingVectorsBuilder.calcRatingVectors(
        regPlaceRatings,
        entityIdColumn = "place_id",
        ratingColumn = "rating",
        vectorColumn = "rating_vector"
      )
      val regCategoryRatingVectors = RatingVectorsBuilder.calcRatingVectors(
        regCategoryRatings,
        entityIdColumn = "category_id",
        ratingColumn = "rating",
        vectorColumn = "rating_vector"
      )

      val placeRatingVectorsFileName = DataUtils.generatePlaceRatingVectorsFileName(regIds, config.dataDir)
      val categoryRatingVectorsFileName = DataUtils.generateCategoryRatingVectorsFileName(regIds, config.dataDir)
      val placeRatingsFileName = DataUtils.generatePlaceRatingsFileName(regIds, config.dataDir)

      writeDf(placeRatingVectorsFileName, regPlaceRatingVectors)
      writeDf(categoryRatingVectorsFileName, regCategoryRatingVectors)
      writeDf(placeRatingsFileName, regPlaceRatings)
    }

  }

  private def writeDf(fileName: String, df: DataFrame): Unit = {
    Console.out.println(s"Writing: $fileName")
    df.write
      .mode(SaveMode.Overwrite)
      .parquet(fileName)
  }

}
