package com.github.tashoyan.visitor.recommender.knn

import com.github.tashoyan.visitor.recommender.PlaceVisits
import org.apache.spark.sql.SparkSession

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
    val places = spark.read
      .parquet(s"${config.samplesDir}/places_sample")

    Console.out.println("Generating place visits")
    val placeVisits = PlaceVisits.calcPlaceVisits(locationVisits, places)
      .cache()
    PlaceVisits.printPlaceVisits(placeVisits)
    //    PlaceVisits.writePlaceVisits(placeVisits, config.samplesDir)

    //TODO Complete
  }

}
