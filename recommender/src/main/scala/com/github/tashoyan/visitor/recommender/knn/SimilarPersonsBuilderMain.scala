package com.github.tashoyan.visitor.recommender.knn

import com.github.tashoyan.visitor.recommender.PlaceVisits
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.LongType
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

object SimilarPersonsBuilderMain extends SimilarPersonsBuilderArgParser {

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
    val placeVisits = PlaceVisits.calcPlaceVisits(locationVisits, places)
      .cache()
    //    PlaceVisits.printPlaceVisits(placeVisits)
    //    PlaceVisits.writePlaceVisits(placeVisits, config.samplesDir)

    Console.out.println("Generating similar persons")
    val similarPersons = new SimilarPersonsBuilder(config.placeWeight, config.categoryWeight, config.kNearest)
      .calcSimilarPersons(placeVisits)
    writeSimilarPersons(similarPersons)
  }

  private def writeSimilarPersons(similarPersons: DataFrame)(implicit config: SimilarPersonsBuilderConfig): Unit = {
    //TODO How to partition?
    similarPersons.write
      .mode(SaveMode.Overwrite)
      .parquet(s"${config.samplesDir}/similar_persons")
  }

}
