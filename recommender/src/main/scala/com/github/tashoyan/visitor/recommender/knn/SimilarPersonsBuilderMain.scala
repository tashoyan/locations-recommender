package com.github.tashoyan.visitor.recommender.knn

import com.github.tashoyan.visitor.recommender.PlaceVisits
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.LongType

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

    val similarPersons = new SimilarPersonsBuilder(config.alphaPlace, config.alphaCategory, config.kNearest)
      .calcSimilarPersons(placeVisits)
    similarPersons.show(false)

    //TODO Complete
  }

}
