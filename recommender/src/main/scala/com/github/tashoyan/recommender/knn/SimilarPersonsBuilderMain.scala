package com.github.tashoyan.recommender.knn

import com.github.tashoyan.recommender.{DataUtils, PlaceVisits}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

object SimilarPersonsBuilderMain extends SimilarPersonsBuilderArgParser with PlaceVisits {

  def main(args: Array[String]): Unit = {
    parser.parse(args, SimilarPersonsBuilderConfig()) match {
      case Some(config) => doMain(config)
      case None => sys.exit(1)
    }
  }

  private def doMain(implicit config: SimilarPersonsBuilderConfig): Unit = {
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

    generateRegionSimilarPersons(placeVisits, places)
  }

  private def generateRegionSimilarPersons(
      placeVisits: DataFrame,
      places: DataFrame
  )(implicit spark: SparkSession, config: SimilarPersonsBuilderConfig): Unit = {
    val regionIds = extractRegionIds(places)
    val regionsPlaceVisits = extractRegionsPlaceVisits(placeVisits, regionIds)
    val similarPersonsBuilder = new SimilarPersonsBuilder(config.placeWeight, config.categoryWeight, config.kNearest)

    regionsPlaceVisits.foreach { case (regIds, regPlaceVisits) =>
      val regPlaceRatings = RatingsBuilder.calcPlaceRatings(regPlaceVisits)
      val regCategoryRatings = RatingsBuilder.calcCategoryRatings(regPlaceVisits)
      val regSimilarPersons = similarPersonsBuilder.calcSimilarPersons(regPlaceRatings, regCategoryRatings)

      val simPersFileName = DataUtils.generateSimilarPersonsFileName(regIds, config.dataDir)
      val placeRatingsFileName = DataUtils.generatePlaceRatingsFileName(regIds, config.dataDir)
      writeSimilarPersons(simPersFileName, regSimilarPersons)
      writePlaceRatings(placeRatingsFileName, regPlaceRatings)
    }
  }

  private def writeSimilarPersons(fileName: String, similarPersons: DataFrame): Unit = {
    Console.out.println(s"Writing similar persons: $fileName")
    similarPersons.write
      .mode(SaveMode.Overwrite)
      .parquet(fileName)
  }

  private def writePlaceRatings(fileName: String, placeRatings: DataFrame): Unit = {
    Console.out.println(s"Writing place ratings: $fileName")
    placeRatings.write
      .mode(SaveMode.Overwrite)
      .parquet(fileName)
  }

}
