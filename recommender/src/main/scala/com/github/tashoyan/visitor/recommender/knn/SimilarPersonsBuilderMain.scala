package com.github.tashoyan.visitor.recommender.knn

import com.github.tashoyan.visitor.recommender.{DataUtils, PlaceVisits}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.LongType
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

    val locationVisits = spark.read
      .parquet(s"${config.samplesDir}/location_visits_sample")
      .withColumn("region_id", col("region_id") cast LongType)
    val places = spark.read
      .parquet(s"${config.samplesDir}/places_sample")
      .withColumn("region_id", col("region_id") cast LongType)

    Console.out.println("Generating place visits")
    val placeVisits = calcPlaceVisits(locationVisits, places)
      .cache()
    //    printPlaceVisits(placeVisits)
    //    writePlaceVisits(placeVisits, config.samplesDir)

    generateRegionSimilarPersons(placeVisits)
  }

  private def generateRegionSimilarPersons(placeVisits: DataFrame)(implicit spark: SparkSession, config: SimilarPersonsBuilderConfig): Unit = {
    val regionsPlaceVisits = extractRegionsPlaceVisits(placeVisits)

    val regionsSimilarPersonsAndPlaceRatings = regionsPlaceVisits
      .map { case (regIds, regPlaceVisits) =>
        val regPlaceRatings = RatingsBuilder.calcPlaceRatings(regPlaceVisits)
        val regCategoryRatings = RatingsBuilder.calcCategoryRatings(regPlaceVisits)
        val regSimilarPersons = generateSimilarPersons(regPlaceRatings, regCategoryRatings)
        (
          DataUtils.generateSimilarPersonsFileName(regIds, config.samplesDir),
          regSimilarPersons,
          DataUtils.generatePlaceRatingsFileName(regIds, config.samplesDir),
          regPlaceRatings
        )
      }
    regionsSimilarPersonsAndPlaceRatings.foreach { case (simPersFileName, similarPersons, placeRatingsFileName, placeRatings) =>
      writeSimilarPersons(simPersFileName, similarPersons)
      writePlaceRatings(placeRatingsFileName, placeRatings)
    }
  }

  private def generateSimilarPersons(
      placeRatings: DataFrame,
      categoryRatings: DataFrame
  )(implicit config: SimilarPersonsBuilderConfig): DataFrame = {
    val similarPersonsBuilder = new SimilarPersonsBuilder(config.placeWeight, config.categoryWeight, config.kNearest)
    similarPersonsBuilder.calcSimilarPersons(placeRatings, categoryRatings)
  }

  private def writeSimilarPersons(fileName: String, similarPersons: DataFrame): Unit = {
    Console.out.println(s"Writing similar persons: $fileName")
    //TODO How to partition?
    similarPersons.write
      .mode(SaveMode.Overwrite)
      .parquet(fileName)
  }

  private def writePlaceRatings(fileName: String, placeRatings: DataFrame): Unit = {
    Console.out.println(s"Writing place ratings: $fileName")
    //TODO How to partition?
    placeRatings.write
      .mode(SaveMode.Overwrite)
      .parquet(fileName)
  }

}
